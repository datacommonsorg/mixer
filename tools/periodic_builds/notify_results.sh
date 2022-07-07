success_dir="/workspace/success/*"
failed_dir="/workspace/failed/*"

cd tools/send_email
for f in $failed_dir
do
	printf "stubbed out email; $(basename $f)\n"
#	go run main.go \
#	  --subject="[Periodic Build Notification] $(basename $f)" \
#	  --receiver="snny@google.com" \
#	  --body_file="$f" \
#	  --mime_type="text/plain"
done

printf "count success: $(ls -l $success_dir | wc -l)\n\n"
printf "success: \n $(ls -l $success_dir)\n\n"

printf "\n\n\n"

printf "count failed: $(ls -l $failed_dir | wc -l)\n\n"
printf "failed: \n $(ls -l $failed_dir)\n\n"
