dev.up:
	podman compose -f docker-compose.dev.yml down
	mkdir -p var/lib
	mkdir -p var/log
	podman compose -f docker-compose.dev.yml up

dev.down:
	podman compose -f docker-compose.dev.yml down

dev.spam:
	sh ./spam_set_requests.sh

dev.clean:
	rm -f var/lib/bureau/*
	rm -f var/log/bureau/*

.PHONY: dev.up dev.down dev.spam dev.clean
