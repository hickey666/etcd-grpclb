package main

import (
	"context"
	"demo/etcd/grpclb/etcdv3"
	pb "demo/etcd/grpclb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"log"
	"strconv"
	"time"
)

var (
	// EtcdEndpoints etcd地址
	EtcdEndpoints = []string{"localhost:2380"}
	// SerName 服务名称
	SerName    = "simple_grpc"
	grpcClient pb.SimpleClient
)

func main() {
	r := etcdv3.NewServiceDiscovery(EtcdEndpoints)
	resolver.Register(r)
	conn, err := grpc.Dial(r.Scheme()+"://8.8.8.8/simple_grpc", grpc.WithBalancerName("round_robin"), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("net.Connect err: %v", err)
	}
	defer conn.Close()

	grpcClient = pb.NewSimpleClient(conn)
	for i := 0; i < 100; i++ {
		route(i)
		time.Sleep(time.Second)
	}
}

func route(i int) {
	req := pb.SimpleRequest{
		Data: "grpc" + strconv.Itoa(i),
	}
	res, err := grpcClient.Route(context.Background(), &req)
	if err != nil {
		log.Fatalf("Call Route err: %v", err)
	}
	log.Println(res)
}