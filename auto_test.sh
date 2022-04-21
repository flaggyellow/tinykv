clearFunc() {
	DES_FOLDER=/tmp
	for FOLDER in $(ls $DES_FOLDER); do
		#  截取test
		test=$(expr substr $FOLDER 1 4)
		if [ "$test" = "test" ]; then
			$(rm -fr $DES_FOLDER/$FOLDER)
		fi
	done
}
echo "---TestStart---" > result.log
for ((i = 1; i <= 100; i++)); do
	echo "testing - round $i" >> result.log
	check_results=$(make project2b)
	# check_results=$( go test -v -run TestConfChangeRecoverManyClients3B ./kv/test_raftstore )
	# check_results=$( go test -v ./scheduler/server -check.f  TestRegionNotUpdate3C )
	$(go clean -testcache)
	clearFunc
	if [[ $check_results =~ "FAIL" ]]; then
		echo "$check_results" > ./test2b_1/round-"$i".log
		echo "!!!round $i failed!!!" >> result.log
		clearFunc
		# break
	fi
	if [[ $check_results =~ "TWOLEADER" ]]; then
		echo "$check_results" > ./test2b_1/notice-"$i".log
		echo "?found error in round $i?" >> result.log
	fi
	# echo "$check_results" > ./test3b/out-"$i".log

done
