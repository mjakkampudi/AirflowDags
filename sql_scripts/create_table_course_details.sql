CREATE TABLE IF NOT EXISTS edx_enrollments.Course_Details as
(SELECT distinct
`Course_id`,
`Course_number`,
`Institution`,
`Course_term`,
`Course_Short_Title` ,
`Course_Long_Title`
 FROM `data-eng1.edx_enrollments.INFORMATION`);