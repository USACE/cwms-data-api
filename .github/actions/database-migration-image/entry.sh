#!/bin/bash

# STOP GAP, need to reset the migration container mappings
export BUILDUSER_PASSWORD=$SYS_PASSWORD
export TEST_ACCOUNT="-notestaccount"

exec $*