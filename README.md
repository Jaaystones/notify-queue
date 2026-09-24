# Notify Queue

Distributed delayed job and notification delivery service.

Full setup and run instructions are written on Day 3. Until then:

```bash
cp .env.example .env        # put your Upstash rediss:// URL in REDIS_URL
make install                # uv sync
make up                     # Postgres (5440) + local Redis (6390) via Docker
make migrate
make api                    # http://localhost:8000/docs
make test
```
