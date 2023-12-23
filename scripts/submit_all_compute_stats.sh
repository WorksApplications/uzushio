submit_post2017() {
    qsub -g gcf51199 -l rt_F=10 -l h_rt=4:00:00 submit_dedup_stage1.sh \
        "/groups/gcf51199/cc/extracted/segment\=$1" \
        /groups/gcf51199/cc/stats_raw_v2/segment=$1 \
        500 4000
}

# submit_post2017 CC-MAIN-2017-04
# submit_post2017 CC-MAIN-2017-09
# submit_post2017 CC-MAIN-2017-13
# submit_post2017 CC-MAIN-2017-17
# submit_post2017 CC-MAIN-2017-22
# submit_post2017 CC-MAIN-2017-26
# submit_post2017 CC-MAIN-2017-30
# submit_post2017 CC-MAIN-2017-34
# submit_post2017 CC-MAIN-2017-39
# submit_post2017 CC-MAIN-2017-43
# submit_post2017 CC-MAIN-2017-47
# submit_post2017 CC-MAIN-2017-51
# submit_post2017 CC-MAIN-2018-05
# submit_post2017 CC-MAIN-2018-09
# submit_post2017 CC-MAIN-2018-13
# submit_post2017 CC-MAIN-2018-17
# submit_post2017 CC-MAIN-2018-22
# submit_post2017 CC-MAIN-2018-26
# submit_post2017 CC-MAIN-2018-30
# submit_post2017 CC-MAIN-2018-34
# submit_post2017 CC-MAIN-2018-39
# submit_post2017 CC-MAIN-2018-43
# submit_post2017 CC-MAIN-2018-47
# submit_post2017 CC-MAIN-2018-51
# submit_post2017 CC-MAIN-2019-04
# submit_post2017 CC-MAIN-2019-09
# submit_post2017 CC-MAIN-2019-13
# submit_post2017 CC-MAIN-2019-18
# submit_post2017 CC-MAIN-2019-22
# submit_post2017 CC-MAIN-2019-26
# submit_post2017 CC-MAIN-2019-30
# submit_post2017 CC-MAIN-2019-35
# submit_post2017 CC-MAIN-2019-39
# submit_post2017 CC-MAIN-2019-43
# submit_post2017 CC-MAIN-2019-47
# submit_post2017 CC-MAIN-2019-51
# submit_post2017 CC-MAIN-2020-05
# submit_post2017 CC-MAIN-2020-10
# submit_post2017 CC-MAIN-2020-16
# submit_post2017 CC-MAIN-2020-24
# submit_post2017 CC-MAIN-2020-29
# submit_post2017 CC-MAIN-2020-34
# submit_post2017 CC-MAIN-2020-40
# submit_post2017 CC-MAIN-2020-45
# submit_post2017 CC-MAIN-2020-50
# submit_post2017 CC-MAIN-2021-04
# submit_post2017 CC-MAIN-2021-10
# submit_post2017 CC-MAIN-2021-17
# submit_post2017 CC-MAIN-2021-21
# submit_post2017 CC-MAIN-2021-25
# submit_post2017 CC-MAIN-2021-31
# submit_post2017 CC-MAIN-2021-39
# submit_post2017 CC-MAIN-2021-43
# submit_post2017 CC-MAIN-2021-49
# submit_post2017 CC-MAIN-2022-05
# submit_post2017 CC-MAIN-2022-21
# submit_post2017 CC-MAIN-2022-27
# submit_post2017 CC-MAIN-2022-33
# submit_post2017 CC-MAIN-2022-40
# submit_post2017 CC-MAIN-2022-49
# submit_post2017 CC-MAIN-2023-06
# submit_post2017 CC-MAIN-2023-14
# submit_post2017 CC-MAIN-2023-23
submit_post2017 CC-MAIN-2023-40