#!/bin/env bash

submit() {
    qsub -g gcf51199 -l rt_F=10 -l h_rt=1:00:00 submit_filter_debug_2.sh \
        "/groups/gcf51199/cc/extracted/segment\=$1" \
        /groups/gcf51199/cc/stats_merged_v2/for_filter/all \
        "/groups/gcf51199/cc/filtered_v3/segment=$1"
}

submit_pre2016() {
    qsub -g gcf51199 -l rt_F=10 -l h_rt=1:00:00 submit_filter_debug_2.sh \
        "/groups/gcf51199/cc2/extracted/$1" \
        /groups/gcf51199/cc/stats_merged_v2/for_filter/all \
        "/groups/gcf51199/cc/filtered_v3/segment=$1"
}

# submit_pre2016 merged-2013
# submit_pre2016 merged-2014
# submit_pre2016 merged-2015
# submit_pre2016 merged-2016

# submit CC-MAIN-2017-04
# submit CC-MAIN-2017-09
# submit CC-MAIN-2017-13
# submit CC-MAIN-2017-17
# submit CC-MAIN-2017-22
# submit CC-MAIN-2017-26
# submit CC-MAIN-2017-30
# submit CC-MAIN-2017-34
# submit CC-MAIN-2017-39
submit CC-MAIN-2017-43
# submit CC-MAIN-2017-47
# submit CC-MAIN-2017-51
# submit CC-MAIN-2018-05
# submit CC-MAIN-2018-09
# submit CC-MAIN-2018-13
# submit CC-MAIN-2018-17
# submit CC-MAIN-2018-22
# submit CC-MAIN-2018-26
# submit CC-MAIN-2018-30
# submit CC-MAIN-2018-34
# submit CC-MAIN-2018-39
# submit CC-MAIN-2018-43
# submit CC-MAIN-2018-47
# submit CC-MAIN-2018-51
# submit CC-MAIN-2019-04
# submit CC-MAIN-2019-09
# submit CC-MAIN-2019-13
# submit CC-MAIN-2019-18
# submit CC-MAIN-2019-22
# submit CC-MAIN-2019-26
# submit CC-MAIN-2019-30
# submit CC-MAIN-2019-35
# submit CC-MAIN-2019-39
# submit CC-MAIN-2019-43
# submit CC-MAIN-2019-47
# submit CC-MAIN-2019-51
# submit CC-MAIN-2020-05
# submit CC-MAIN-2020-10
# submit CC-MAIN-2020-16
# submit CC-MAIN-2020-24
# submit CC-MAIN-2020-29
# submit CC-MAIN-2020-34
# submit CC-MAIN-2020-40
# submit CC-MAIN-2020-45
# submit CC-MAIN-2020-50
# submit CC-MAIN-2021-04
# submit CC-MAIN-2021-10
# submit CC-MAIN-2021-17
# submit CC-MAIN-2021-21
# submit CC-MAIN-2021-25
# submit CC-MAIN-2021-31
# submit CC-MAIN-2021-39
# submit CC-MAIN-2021-43
# submit CC-MAIN-2021-49
# submit CC-MAIN-2022-05
# submit CC-MAIN-2022-21
# submit CC-MAIN-2022-27
# submit CC-MAIN-2022-33
# submit CC-MAIN-2022-40
# submit CC-MAIN-2022-49
# submit CC-MAIN-2023-06
# submit CC-MAIN-2023-14
# submit CC-MAIN-2023-23
# submit CC-MAIN-2023-40