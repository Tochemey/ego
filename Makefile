.PHONY: run-eventsourced run-durablestate run-saga proto

# Run the event-sourced example
run-eventsourced:
	@echo "Running event-sourced example..."
	go run ./example/eventssourced

# Run the durable state example
run-durablestate:
	@echo "Running durable state example..."
	go run ./example/durablestate

# Run the fund transfer saga example
run-saga:
	@echo "Running fund transfer saga example..."
	go run ./example/saga

# Regenerate protobuf code
proto:
	@echo "Generating protobuf code..."
	buf generate
	cp gen/sample/sample.pb.go example/examplepb/sample.pb.go
	rm -rf gen
	@echo "Done."
