file=$1
slaves=$2
./test.sh $file BruteForce $slaves
./test.sh $file HCA $slaves
./test.sh $file improvedHCA $slaves

