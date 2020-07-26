#!/bin/bash
#
#
aws sqs send-message --queue-url https://sqs.ap-southeast-2.amazonaws.com/137834070286/git --region ap-southeast-2 --message-body $1

