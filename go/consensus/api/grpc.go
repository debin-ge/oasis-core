package api

import (
	"context"

	"google.golang.org/grpc"

	cmnGrpc "github.com/oasislabs/oasis-core/go/common/grpc"
	"github.com/oasislabs/oasis-core/go/common/pubsub"
	"github.com/oasislabs/oasis-core/go/consensus/api/transaction"
	epochtime "github.com/oasislabs/oasis-core/go/epochtime/api"
	genesis "github.com/oasislabs/oasis-core/go/genesis/api"
)

var (
	// serviceName is the gRPC service name.
	serviceName = cmnGrpc.NewServiceName("Consensus")

	// methodSubmitTx is the name of the SubmitTx method.
	methodSubmitTx = serviceName.NewMethodName("SubmitTx")
	// methodStateToGenesis is the name of the StateToGenesis method.
	methodStateToGenesis = serviceName.NewMethodName("StateToGenesis")
	// methodWaitEpoch is the name of the WaitEpoch method.
	methodWaitEpoch = serviceName.NewMethodName("WaitEpoch")
	// methodGetBlock is the name of the GetBlock method.
	methodGetBlock = serviceName.NewMethodName("GetBlock")
	// methodGetTransactions is the name of the GetTransactions method.
	methodGetTransactions = serviceName.NewMethodName("GetTransactions")

	// methodWatchBlocks is the name of the WatchBlocks method.
	methodWatchBlocks = serviceName.NewMethodName("WatchBlocks")

	// serviceDesc is the gRPC service descriptor.
	serviceDesc = grpc.ServiceDesc{
		ServiceName: string(serviceName),
		HandlerType: (*Backend)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: methodSubmitTx.Short(),
				Handler:    handlerSubmitTx,
			},
			{
				MethodName: methodStateToGenesis.Short(),
				Handler:    handlerStateToGenesis,
			},
			{
				MethodName: methodWaitEpoch.Short(),
				Handler:    handlerWaitEpoch,
			},
			{
				MethodName: methodGetBlock.Short(),
				Handler:    handlerGetBlock,
			},
			{
				MethodName: methodGetTransactions.Short(),
				Handler:    handlerGetTransactions,
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    methodWatchBlocks.Short(),
				Handler:       handlerWatchBlocks,
				ServerStreams: true,
			},
		},
	}
)

func handlerSubmitTx( // nolint: golint
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	rq := new(transaction.SignedTransaction)
	if err := dec(rq); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return nil, srv.(Backend).SubmitTx(ctx, rq)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: methodSubmitTx.Full(),
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, srv.(Backend).SubmitTx(ctx, req.(*transaction.SignedTransaction))
	}
	return interceptor(ctx, rq, info, handler)
}

func handlerStateToGenesis( // nolint: golint
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	var height int64
	if err := dec(&height); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(Backend).StateToGenesis(ctx, height)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: methodStateToGenesis.Full(),
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(Backend).StateToGenesis(ctx, req.(int64))
	}
	return interceptor(ctx, height, info, handler)
}

func handlerWaitEpoch( // nolint: golint
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	var epoch epochtime.EpochTime
	if err := dec(&epoch); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return nil, srv.(Backend).WaitEpoch(ctx, epoch)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: methodWaitEpoch.Full(),
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, srv.(Backend).WaitEpoch(ctx, req.(epochtime.EpochTime))
	}
	return interceptor(ctx, epoch, info, handler)
}

func handlerGetBlock( // nolint: golint
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	var height int64
	if err := dec(&height); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(Backend).GetBlock(ctx, height)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: methodGetBlock.Full(),
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(Backend).GetBlock(ctx, req.(int64))
	}
	return interceptor(ctx, height, info, handler)
}

func handlerGetTransactions( // nolint: golint
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	var height int64
	if err := dec(&height); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(Backend).GetTransactions(ctx, height)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: methodGetTransactions.Full(),
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(Backend).GetTransactions(ctx, req.(int64))
	}
	return interceptor(ctx, height, info, handler)
}

func handlerWatchBlocks(srv interface{}, stream grpc.ServerStream) error {
	if err := stream.RecvMsg(nil); err != nil {
		return err
	}

	ctx := stream.Context()
	ch, sub, err := srv.(Backend).WatchBlocks(ctx)
	if err != nil {
		return err
	}
	defer sub.Close()

	for {
		select {
		case blk, ok := <-ch:
			if !ok {
				return nil
			}

			if err := stream.SendMsg(blk); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RegisterService registers a new consensus backend service with the
// given gRPC server.
func RegisterService(server *grpc.Server, service Backend) {
	server.RegisterService(&serviceDesc, service)
}

type consensusClient struct {
	conn *grpc.ClientConn
}

func (c *consensusClient) SubmitTx(ctx context.Context, tx *transaction.SignedTransaction) error {
	return c.conn.Invoke(ctx, methodSubmitTx.Full(), tx, nil)
}

func (c *consensusClient) StateToGenesis(ctx context.Context, height int64) (*genesis.Document, error) {
	var rsp genesis.Document
	if err := c.conn.Invoke(ctx, methodStateToGenesis.Full(), height, &rsp); err != nil {
		return nil, err
	}
	return &rsp, nil
}

func (c *consensusClient) WaitEpoch(ctx context.Context, epoch epochtime.EpochTime) error {
	return c.conn.Invoke(ctx, methodWaitEpoch.Full(), epoch, nil)
}

func (c *consensusClient) GetBlock(ctx context.Context, height int64) (*Block, error) {
	var rsp Block
	if err := c.conn.Invoke(ctx, methodGetBlock.Full(), height, &rsp); err != nil {
		return nil, err
	}
	return &rsp, nil
}

func (c *consensusClient) GetTransactions(ctx context.Context, height int64) ([][]byte, error) {
	var rsp [][]byte
	if err := c.conn.Invoke(ctx, methodGetTransactions.Full(), height, &rsp); err != nil {
		return nil, err
	}
	return rsp, nil
}

func (c *consensusClient) WatchBlocks(ctx context.Context) (<-chan *Block, pubsub.ClosableSubscription, error) {
	ctx, sub := pubsub.NewContextSubscription(ctx)

	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[0], methodWatchBlocks.Full())
	if err != nil {
		return nil, nil, err
	}
	if err = stream.SendMsg(nil); err != nil {
		return nil, nil, err
	}
	if err = stream.CloseSend(); err != nil {
		return nil, nil, err
	}

	ch := make(chan *Block)
	go func() {
		defer close(ch)

		for {
			var blk Block
			if serr := stream.RecvMsg(&blk); serr != nil {
				return
			}

			select {
			case ch <- &blk:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, sub, nil
}

// NewConsensusClient creates a new gRPC consensus client service.
func NewConsensusClient(c *grpc.ClientConn) ClientBackend {
	return &consensusClient{c}
}
