#!/bin/bash
s3-to-dc "s3://deafrica-services/wofs_ls_summary_alltime/1-0-0/x211/y077/1984--P40Y/wofs_ls_summary_alltime_x211y077_1984--P40Y.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls_summary_alltime'
s3-to-dc "s3://deafrica-services/wofs_ls_summary_alltime/1-0-0/x211/y076/1984--P40Y/wofs_ls_summary_alltime_x211y076_1984--P40Y.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls_summary_alltime'
s3-to-dc "s3://deafrica-services/wofs_ls_summary_alltime/1-0-0/x210/y076/1984--P40Y/wofs_ls_summary_alltime_x210y076_1984--P40Y.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls_summary_alltime'
# s3-to-dc "s3://deafrica-services/wofs_ls_summary_alltime/1-0-0/x217/y077/1984--P40Y/wofs_ls_summary_alltime_x217y077_1984--P40Y.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls_summary_alltime'
# wofs_ls dataset to use for testing the surface area change
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/176/083/2016/04/05/wofs_ls_176083_2016-04-05.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/176/082/2016/04/05/wofs_ls_176082_2016-04-05.stac-item.json" --stac --no-sign-request --skip-lineage 'wofs_ls'