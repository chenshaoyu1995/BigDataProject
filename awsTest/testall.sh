root='/home/ec2-user/BigDataProject'
test="$root/awsTest/test.sh"
file=$1
slaves=$3
./$test $file BruteForce $slaves
./$test $file HCA $slaves
./$test $file improvedHCA $slaves

