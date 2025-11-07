
.PHONY: cover

cover:
	uv run --with pytest-cov pytest --cov
