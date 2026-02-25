#!/usr/bin/env bash
# bootstrap.sh - Prepara o ambiente do Global Store Pipeline para execucao local.
# Uso: source bootstrap.sh   (ou: . bootstrap.sh)

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Global Store Pipeline - Bootstrap"
echo "Projeto: $PROJECT_DIR"

# 1. PYTHONPATH
if [[ ":$PYTHONPATH:" != *":$PROJECT_DIR:"* ]]; then
    export PYTHONPATH="$PYTHONPATH:$PROJECT_DIR"
    echo "[OK] PYTHONPATH configurado."
else
    echo "[INFO] PYTHONPATH ja contem o diretorio do projeto."
fi

# 2. Symlinks das DAGs para ~/airflow/dags
AIRFLOW_DAGS_DIR="$HOME/airflow/dags"
mkdir -p "$AIRFLOW_DAGS_DIR"

for dag_file in "$PROJECT_DIR/dags/"*.py; do
    dag_name="$(basename "$dag_file")"
    target="$AIRFLOW_DAGS_DIR/$dag_name"

    if [ -L "$target" ]; then
        echo "[INFO] Symlink ja existe: $dag_name"
    else
        ln -sf "$dag_file" "$target"
        echo "[OK] Symlink criado: $dag_name -> $AIRFLOW_DAGS_DIR/"
    fi
done

# 3. Carrega .env no shell atual
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$PROJECT_DIR/.env"
    set +a
    echo "[OK] Variaveis do .env carregadas no shell."
fi

echo ""
echo "Ambiente pronto! Para iniciar o Airflow:"
echo "   cd $PROJECT_DIR && poetry run airflow standalone"
echo ""
echo "Ordem de execucao das DAGs:"
echo "   1. setup_database      -> cria as tabelas (rodar uma vez)"
echo "   2. check_connection    -> valida conectividade"
echo "   3. bronze_extract      -> extrai da FakeStoreAPI"
echo "   4. silver_transform    -> transforma os dados"
echo "   5. load_validate       -> valida o resultado final"
