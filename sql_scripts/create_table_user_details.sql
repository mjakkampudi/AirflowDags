CREATE TABLE IF NOT EXISTS edx_enrollments.User_Details as
(SELECT distinct `Userid_DI`,
`Country`,
`LoE_DI` ,
`YoB` ,
`Age` ,
`Gender`
FROM `data-eng1.edx_enrollments.INFORMATION`);