# test:
	# TODO

# test.integration:
	# TODO

# bench:
	# TODO

dev.run:
	podman compose -f docker-compose.dev.yml down
	mkdir -p var/lib
	mkdir -p var/log
	podman compose -f docker-compose.dev.yml up

dev.down:
	podman compose -f docker-compose.dev.yml down

.PHONY: dev.run dev.down
