package main

import (
	"context"
	"demo/etcd/grpclb/etcdv3"
	"google.golang.org/grpc"
	"log"
	"net"
	pb "demo/etcd/grpclb/proto"
)

type SimpleService struct {
}

const (
	Address string = "localhost:8082"
	Network string = "tcp"
	SerName string = "simple_grpc"
)

var EtcEndpoints = []string{"localhost:2380"}

func main() {
	listener, err := net.Listen(Network, Address)
	if err != nil {
		log.Fatalf("net.listen err: %v", err)
	}
	log.Println(Address + " net.Listing...")
	grpcServer := grpc.NewServer()
	pb.RegisterSimpleServer(grpcServer, &SimpleService{})
	ser, err := etcdv3.NewServiceRegister(EtcEndpoints, SerName, Address, 5)
	if err != nil {
		log.Fatalf("register service err: %v", err)
	}
	defer ser.Close()
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("grpcServer.Serve err: %v", err)
	}
}

func (s *SimpleService) Route(ctx context.Context, req *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	log.Println("receive: " + req.Data)
	res := pb.SimpleResponse{
		Code: 200,
		Value: "hello " + req.Data,
	}
	return &res, nil
}