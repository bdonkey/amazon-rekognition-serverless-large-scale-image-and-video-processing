echo "Copying lambda functions..."
cp helper.py ../tkcimg-pipeline/lambda/helper/python/helper.py
cp datastore.py ../tkcimg-pipeline/lambda/helper/python/datastore.py
cp s3proc.py ../tkcimg-pipeline/lambda/s3processor/lambda_function.py
cp s3batchproc.py ../tkcimg-pipeline/lambda/s3batchprocessor/lambda_function.py
cp itemproc.py ../tkcimg-pipeline/lambda/itemprocessor/lambda_function.py
cp syncproc.py ../tkcimg-pipeline/lambda/syncprocessor/lambda_function.py
cp asyncproc.py ../tkcimg-pipeline/lambda/asyncprocessor/lambda_function.py
cp jobresultsproc.py ../tkcimg-pipeline/lambda/jobresultprocessor/lambda_function.py
cp s3FolderCreator.py ../tkcimg-pipeline/lambda/s3FolderCreator/lambda_function.py

echo "Done!"