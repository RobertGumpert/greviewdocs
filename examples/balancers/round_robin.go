package balancers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var errFailureByBackpressure = errors.New("backpressure: no free nodes")

const numberNodeStatSlots = 5

type readCloserResponse struct {
	close func(*readCloserResponse)
	*bytes.Buffer
}

func (rc *readCloserResponse) Close() error {
	rc.close(rc)
	return nil
}

type node struct {
	id int
	t  *transport

	// флаг владения нодой. в один момент
	// времени нодой может владеть только одна пользовательская
	// горутина или же один запрос
	own atomic.Int32

	// храним стату по отклику ноды
	// храним последние 5 обращений (numberNodeStatSlots)
	respondMs     []int64
	respondErrors []bool
}

func newNode(
	id int,
	t *transport,
) *node {
	n := node{
		id:            id,
		t:             t,
		own:           atomic.Int32{},
		respondMs:     make([]int64, 0, numberNodeStatSlots),
		respondErrors: make([]bool, 0, numberNodeStatSlots),
	}
	return &n
}

type RoundRobin struct {
	ctx context.Context
	cfg ConnetionPoolConfig

	// cfg.Responses.Buffer.InitSize (дефолт 4096)
	respDataPool sync.Pool
	respPool     sync.Pool

	// cfg.Connections.Active.Number
	nodes            []*node
	ringMonotonicCnt int64
	brokenNodes      chan *node
}

// ЭТО НЕ PROD READY ВЕРСИЯ она многое не реализует
// 1) добавление и удаление node
// 2) обновление конфигов
// 3) пинг нод в отдельной горутине чтобы сервак не прибил долго неиспользуемые connection'ы
//
// Это не Балансировщик в чистом виде - это ConnectionPool, чтобы быть Балансировзкиом надо
// уметь наливать еще коннекшенов про росте нагрузки и убирать их если нагрузка снижается
func NewRoundRobin(ctx context.Context, cfg ConnetionPoolConfig, ips []net.IP, connectFn ConnectCallback) (*RoundRobin, error) {

	nodes := make([]*node, 0, len(ips)*cfg.Connections.Active.Number)
	var idx int
	for _, ip := range ips {
		for range cfg.Connections.Active.Number {

			node, err := createNode(ctx, idx, cfg, ip, connectFn)
			if err != nil {
				for _, n := range nodes {
					_ = n.t.conn.Close()
				}

				return nil, err
			}

			idx++
			nodes = append(nodes, node)
		}
	}

	rb := RoundRobin{
		ctx: ctx,
		cfg: cfg,
		respDataPool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, cfg.Responses.Buffer.InitSize))
			},
		},
		respPool: sync.Pool{
			New: func() any {
				return new(readCloserResponse)
			},
		},
		nodes:            nodes,
		brokenNodes:      make(chan *node, len(ips)*cfg.Connections.Active.Number),
		ringMonotonicCnt: -1,
	}

	go rb.brokenNodesReconnector()

	return &rb, nil
}

func (r *RoundRobin) NonIdempotentRequest(ctx context.Context, req io.ReadCloser) (io.ReadCloser, error) {

	node, err := r.broadcast(r.cfg.Arena.AttemptsMax, r.cfg.Arena.WaitDur)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = req.Close()
		r.refuseNodeOwnership(node)
	}()

	resp := r.responseAcquirePool()
	// так как пользовательские ошибки по типу HTTP 404
	// здесь получить не можем, ибо так вот автором задуман API между:
	//
	// 		user code ->
	// 			balancer.send(req, resp) ->
	// 				transport.send(req, resp) ->
	// 			balancer.send (req, resp) ->
	//		user code
	// 			resp.Read() -> HTTP 404
	//
	// то из t.conn должны "вылетать" только сетевые ошибки
	err = r.request(ctx, node, req, resp)
	if err != nil {
		r.responseReleasePool(resp)
		// Мы не знаем что именно произошло внутри Connection
		// поэтому прибегаем к самому жесткому пути - реконнекту.
		// Внутри Connection может быть баг по типу не нашли символ
		// значит бросаем все и выходим, но сервер продолжает пакеты отправлять
		// Что делать? придумывать костыли из-за ошибки в Connection?
		// А может быть это действительно сетевая ошибка - которое не позволит
		// дальше писать или читать с сокета ?
		r.registerBrokenNode(node)
		return nil, err
	}

	return resp, nil
}

func (r *RoundRobin) IdempotentRequest(ctx context.Context, reqFabric ReadCloserFabric) (io.ReadCloser, error) {

	var attempts int
	var afrerReqErr error
	var beforeReqErr error
	node, err := r.broadcast(r.cfg.Arena.AttemptsMax, r.cfg.Arena.WaitDur)
	if err != nil {
		return nil, err
	}
	defer func() {
		r.refuseNodeOwnership(node)
	}()

	resp := r.responseAcquirePool()

	for {
		if attempts >= r.cfg.Requests.Retry.AttemptsMax {
			beforeReqErr = errRetryAttemptsLimitReached
			break
		}

		req, err := reqFabric()
		if err != nil {
			beforeReqErr = err
			break
		}

		afrerReqErr = r.request(ctx, node, req, resp)
		if afrerReqErr == nil {
			return resp, nil
		}

		// если скис context клиента то просто выходим
		if ctx.Err() != nil {
			break
		}
		afrerReqErr = r.reconnectNodeWithJitter(node)
		if afrerReqErr != nil {
			break
		}
		// сбрасываем буффер для ответа
		resp.Buffer.Reset()
	}

	r.responseReleasePool(resp)

	if beforeReqErr != nil {
		return nil, beforeReqErr
	}

	// Мы не знаем что именно произошло внутри Connection
	// поэтому прибегаем к самому жесткому пути - реконнекту.
	// Внутри Connection может быть баг по типу не нашли символ
	// значит бросаем все и выходим, но сервер продолжает пакеты отправлять
	// Что делать? придумывать костыли из-за ошибки в Connection?
	// А может быть это действительно сетевая ошибка - которое не позволит
	// дальше писать или читать с сокета ?
	r.registerBrokenNode(node)

	return nil, afrerReqErr
}

// это сердце нашего Round Robin'а
// мы делеаем "шировещательный" опрос всех нод (он же broadcast)
// и найдеимся что у нас получится зазватить ноду
func (r *RoundRobin) broadcast(attemptsMax int, waitDur time.Duration) (*node, error) {

	var lenN int = len(r.nodes)

	for attempt := 0; attempt < attemptsMax; attempt++ {
		// hot path через ring buffer :)
		// lastPickIdx вечно двигается вперед но при получении
		// остатка от деления мы получим позицию в слайсе
		// 		lenN := 3
		// 		for idx := 0; idx < 100; idx++ {
		// 			idx[ 0 ] ->  0
		// 			idx[ 1 ] ->  1
		// 			idx[ 2 ] ->  2
		// 			idx[ 3 ] ->  0
		// 			idx[ 4 ] ->  1
		// 			idx[ 5 ] ->  2
		// 			fmt.Println("idx[", idx, "] -> ", idx%lenN)
		// 		}
		idx := atomic.AddInt64(&r.ringMonotonicCnt, 1) % int64(lenN)
		//		lenN := 3
		//      idx = 0 -> pickIdx'ы = [0,1,2]
		//      idx = 1 -> pickIdx'ы = [1,2,0]
		// 		idx = 2 -> pickIdx'ы = [2,0,1]
		for ydx := int64(0); ydx < int64(lenN); ydx++ {
			pickIdx := (idx + ydx) % int64(lenN)
			if win := r.arena(r.nodes[pickIdx]); win {
				return r.nodes[pickIdx], nil
			}
		}

		// slow path :)
		if attempt >= attemptsMax/2 && attemptsMax > 3 {
			// паркуем пользовательскую G со всеми вытекающими
			time.Sleep(waitDur)
		}
	}

	return nil, errFailureByBackpressure
}

// арена :) здесь горутины дерутся за право
// забрать себе ноду
func (r *RoundRobin) arena(n *node) bool {
	ownerChance := n.own.Load()
	if ownerChance > 0 {
		return false
	}

	meOwner := ownerChance + 1

	return n.own.CompareAndSwap(ownerChance, meOwner)
}

func (r *RoundRobin) refuseNodeOwnership(n *node) {
	_ = n.own.Add(-1)
}

func (r *RoundRobin) transferNodeOwnership(n *node) {
	_ = n.own.Add(1)
}

func (r *RoundRobin) request(ctx context.Context, n *node, req io.ReadCloser, resp *readCloserResponse) error {

	if len(n.respondMs) == numberNodeStatSlots {
		n.respondMs = n.respondMs[:0]
		n.respondErrors = n.respondErrors[:0]
	}

	now := time.Now()

	err := n.t.conn.Send(ctx, req, resp)

	n.respondMs = append(n.respondMs, time.Since(now).Milliseconds())

	if err != nil {
		n.respondErrors = append(n.respondErrors, true)
	}

	n.respondErrors = append(n.respondErrors, false)

	return nil
}

func (r *RoundRobin) registerBrokenNode(n *node) {

	// перехватываем владение нодой
	r.transferNodeOwnership(n)

	// мы не можем позволить себе блокировать клиента
	go func() {
		// здесь никаких select {case r.brokenNodes case timer.c}
		// позволить себе не можем, потому что это опасно
		// для баласировшика мы должны во чтобы это не стало
		// отправитб в канал сломанную ноду
		select {
		case <-r.ctx.Done():
		case r.brokenNodes <- n:
		}
	}()
}

func (r *RoundRobin) brokenNodesReconnector() {
	for {
		select {
		case <-r.ctx.Done():
			return

		case brokenNode := <-r.brokenNodes:

			_ = brokenNode.t.conn.Close()

			// До упора пытаемся восстановить ноду :)
			// Если сервер умер здесь мы замерм навсегда
			// автор устал ему лень писать prod решение :)
			for {
				err := r.reconnectNodeWithJitter(brokenNode)
				if err == nil {
					break
				}

				time.Sleep(r.cfg.Connections.Jitter.DurMax)
			}

			// отдаем владение этой нодой
			r.refuseNodeOwnership(brokenNode)
		}
	}
}

func (r *RoundRobin) responseAcquirePool() *readCloserResponse {
	resp := r.respPool.Get().(*readCloserResponse)
	data := r.respDataPool.Get().(*bytes.Buffer)

	resp.Buffer = data
	resp.close = r.responseReleasePool

	return resp
}

func (r *RoundRobin) responseReleasePool(resp *readCloserResponse) {
	resp.Buffer.Reset()

	if resp.Buffer.Cap() < r.cfg.Responses.Buffer.MaxSize {
		r.respDataPool.Put(resp.Buffer)
	}

	resp.Buffer = nil
	resp.close = nil

	r.respPool.Put(resp)
}

func (r *RoundRobin) reconnectNodeWithJitter(n *node) error {
	timeoutedCtx, cancel := context.WithTimeout(r.ctx, r.cfg.Connections.Timeout)
	defer cancel()

	return n.t.connectWithJitterRetry(
		timeoutedCtx,
		r.cfg.Connections.Jitter.AttemptsMax,
		r.cfg.Connections.Jitter.DurBase,
		r.cfg.Connections.Jitter.DurMax,
	)
}

func createNode(
	ctx context.Context,
	id int,
	cfg ConnetionPoolConfig,
	ip net.IP,
	connectFn ConnectCallback,
) (*node, error) {

	t := transport{}
	if err := configureTransport(&t, ip, connectFn); err != nil {
		return nil, err
	}

	node := newNode(id, &t)

	timeoutedCtx, cancel := context.WithTimeout(ctx, cfg.Connections.Timeout)
	defer cancel()

	if err := node.t.connectWithJitterRetry(
		timeoutedCtx,
		cfg.Connections.Jitter.AttemptsMax,
		cfg.Connections.Jitter.DurBase,
		cfg.Connections.Jitter.DurMax,
	); err != nil {
		return nil, err
	}

	return node, nil
}
