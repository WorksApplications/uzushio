submit_pre2016() {
    qsub -g gcf51199 -l rt_F=10 -l h_rt=4:00:00 submit_dedup_stage1.sh \
        "/groups/gcf51199/cc2/extracted/$1" \
        "/groups/gcf51199/cc/stats_raw_v2/segment=$1" \
        500 4000
}

submit_pre2016 merged-2013
submit_pre2016 merged-2014
submit_pre2016 merged-2015
submit_pre2016 merged-2016