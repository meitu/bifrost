gosrc = conn/conn.pb.go push/push.pb.go callback/callback.pb.go publish/publish.pb.go
all: $(gosrc)

%.pb.go:%.proto
	protoc --gofast_out=plugins=grpc:. $<

.PHONY: clean
clean:
	rm -rf $(gosrc)
