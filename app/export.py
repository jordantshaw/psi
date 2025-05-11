import os
from pathlib import Path

import pandas as pd

from app.sync import engine

export_dir = Path(os.getenv("EXPORT_DIR"))
queries = {
    'student_skill_summary': '''
        select student.id
             , student.student_name
             , student.grade
             , student.teacher
             , skill.main_skill
             , sum(case when score.correct = true then 1 else 0 end) as correct
             , sum(case when score.correct = false then 1 else 0 end) as incorrect
             , sum(case when score.correct = true then 1 else 0 end) >= 8 as passed
        from score
        inner join student on student.id = score.student_id
        inner join word on word.word = score.word
            inner join skill on skill.skill = word.skill
        where correct is not null and cycle = 1
        group by student.id
               , student.student_name
               , student.grade
               , student.teacher
               , skill.main_skill
        order by skill.main_skill
    '''
}


skill_summary_df = pd.read_sql(queries['student_skill_summary'], con=engine, dtype={'passed': bool})
skill_summary_df.to_csv(export_dir / 'student_skill_summary.csv', index=False)