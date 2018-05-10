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

spark-submit --deploy-mode cluster --master yarn --num-executors $slaves $algFile $inputFile | python $filterFile > $outputFile
