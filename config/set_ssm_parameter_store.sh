#!/bin/bash

aws ssm put-parameter \
  --name "/box/sample/key_config" \
  --type "SecureString" \
  --value file://./box_key_config.json
