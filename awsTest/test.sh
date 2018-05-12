file=$1
alg=$2
slaves=$3
root='/home/ec2-user/BigDataProject'
testRoot="$root/awsTest/$file"
inputFile="$testRoot/$file.csv"
outputFile="$testRoot/$alg-$file-$slaves-slaves.out"
algFile="$root/$alg.py"
filterFile="$root/awsTest/filter.py"
echo "testing $alg on $file with $slaves slaves"

echo $inputFile
echo $outputFile
echo $algFile

spark-submit \
--master=spark://ec2-204-236-192-147.compute-1.amazonaws.com:7077
--driver-memory 1G \
--executor-cores 4 \
--executor-memory 8G \
--num-executors $slaves \
--conf spark.default.parallelism=1000 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \
$algFile $inputFile | python $filterFile > $outputFile

  