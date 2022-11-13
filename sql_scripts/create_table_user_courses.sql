CREATE TABLE IF NOT EXISTS edx_enrollments.User_Course_Registration as
(SELECT `Course_id`,
`Userid_DI`,
`Registered` ,
`Viewed` ,
`Explored` ,
`Certified`,
`Grade` ,
`nevents` ,
`ndays_act` ,
`nplay_video` ,
`nchapters` ,
`nforum_posts` ,
`roles`,
`incomplete_flag`
FROM `data-eng1.edx_enrollments.INFORMATION`);