#!/bin/bash

# wofs_ls dataset to use for testing the surface area change
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/176/083/2016/04/05/wofs_ls_176083_2016-04-05.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/176/082/2016/04/05/wofs_ls_176082_2016-04-05.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls'