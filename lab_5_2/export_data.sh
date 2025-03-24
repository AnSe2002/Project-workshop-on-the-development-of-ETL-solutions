#!/bin/bash

# Параметры
CONTAINER_NAME="business_case_rocket-scheduler-1"
DAG_ID="download_rocket_launches"
TASK_ID="get_pictures"
HOST_OUTPUT_DIR="$HOME/Downloads/airflow_output" 

# Создание директории для выгрузки
mkdir -p "${HOST_OUTPUT_DIR}"

echo "Поиск последних логов..."
LATEST_LOG=$(docker exec $CONTAINER_NAME bash -c "ls -td /opt/airflow/logs/${DAG_ID}/${TASK_ID}/*/ | head -1")/1.log

if [ -n "$LATEST_LOG" ]; then
    echo "Копирование логов из ${LATEST_LOG}..."
    docker cp "${CONTAINER_NAME}:${LATEST_LOG}" "${HOST_OUTPUT_DIR}/logs.zip" && \
        echo "Логи успешно скопированы в ${HOST_OUTPUT_DIR}/logs.zip" || \
        echo "Ошибка копирования логов"
else
    echo "Логи не найдены. Убедитесь, что DAG выполнялся."
fi

# 2. Копирование изображений
echo "Копирование изображений из /tmp/images..."
docker cp "${CONTAINER_NAME}:/tmp/images" "${HOST_OUTPUT_DIR}/" && \
    echo "Изображения успешно скопированы в ${HOST_OUTPUT_DIR}/images" || \
    echo "Ошибка копирования изображений (возможно задача не выполнялась)"

# 3. Проверка результата
echo -e "\nСодержимое ${HOST_OUTPUT_DIR}:"
ls -lh "${HOST_OUTPUT_DIR}"