# -----------------------
# Start everything (dev mode)
# -----------------------
all:
	@echo "🚀 Starting all services (recommended: separate terminals)..."
	make mlflow & \
	make dagster & \
	make api & \
	make lakefs-start


stop-all:
	@echo "🛑 Stopping all services..."
	-@pkill -f "mlflow server" || true
	-@pkill -f "dagster dev" || true
	-@pkill -f "uvicorn src.api.main:app" || true
	@make lakefs-stop


clean:
	find . -type d \( -name "__pycache__" -o -name ".ruff_cache" \) -exec rm -rf {} +

# -----------------------
# MLflow
# -----------------------
mlflow:
	@echo "📦 Starting MLflow UI..."
	mlflow server \
		--host 127.0.0.1 \
		--port 5000 \
		--backend-store-uri sqlite:///mlflow.db


# -----------------------
# Dagster
# -----------------------
dagster:
	@echo "🎛️ Starting Dagster Web UI..."
	export DAGSTER_HOME=$$(pwd) && \
	dagster dev -f src/bike_rental/definitions.py


# -----------------------
# FastAPI
# -----------------------
api:
	@echo "⚡ Starting FastAPI Endpoint..."
	uvicorn src.api.main:app --host 127.0.0.1 --port 8001 --reload


# -----------------------
# LakeFS (Podman - persistent container)
# -----------------------

lakefs-create:
	@echo "🧱 Creating LakeFS container (only first time)..."
	podman create \
		--name lakefs \
		--restart=always \
		-p 8000:8000 \
		-v "$$PWD/lakefs-local/config.yaml:/etc/lakefs/config.yaml:Z" \
		-v "$$PWD/lakefs-local/data:/lakefs/data:Z" \
		treeverse/lakefs:latest \
		run --config /etc/lakefs/config.yaml || true


lakefs-start:
	@echo "🚀 Starting LakeFS..."
	podman start lakefs


lakefs-stop:
	@echo "🛑 Stopping LakeFS..."
	podman stop lakefs || true


lakefs-logs:
	@echo "📜 LakeFS logs..."
	podman logs -f lakefs


# -----------------------
# Status check
# -----------------------
status:
	@echo "📊 Service Status"
	@echo "-----------------------------"
	@make -s lakefs-status || true
	@echo ""
	@echo "Other services (manual check): mlflow / dagster / api"


lakefs-status:
	@podman ps -a --filter name=lakefs

.PHONY: mlflow dagster api all stop lakefs-create lakefs-start lakefs-stop lakefs-logs status clean
