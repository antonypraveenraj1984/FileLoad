spark-submit --master yarn \
            --queue default \
            --deploy-mode cluster \
            --class FileLoad \
            /src/main/auth.jar