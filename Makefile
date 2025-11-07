.PHONY: cover serve-docs deploy-docs

cover:
	uv run --with pytest-cov pytest --cov

serve-docs:
	uv run mkdocs serve

deploy-docs:
	uv run mkdocs gh-deploy
