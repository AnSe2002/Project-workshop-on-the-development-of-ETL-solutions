#!/bin/bash
LOCAL_DIR="/home/mgpu/Downloads/image_28_03"
SOURCE_DIR="/home/mgpu/Downloads/test2/workshop-on-ETL/business_case_rocket_25/data"

mkdir -p "$LOCAL_DIR"
cp -r "$SOURCE_DIR/"* "$LOCAL_DIR/"  
find "$LOCAL_DIR" -type f ! \( -name '*.png' -o -name '*.jpg' -o -name '*.jpeg' \) -delete
echo "Файлы скопированы в $LOCAL_DIR"