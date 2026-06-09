clean:
	find . -type d \( -name "__pycache__" -o -name ".ruff_cache" \) -exec rm -rf {} +