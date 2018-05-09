test="./home/ec2-user/BigDataProject/awsTest/test.sh"
file=$1
slaves=$3
$test $file BruteForce $slaves
$test $file HCA $slaves
$test $file improvedHCA $slaves

