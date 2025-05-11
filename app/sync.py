import os

import pandas as pd

from .utils import get_google_sheet_data
from .db import get_db_engine


engine = get_db_engine()
file_id = os.getenv('SOURCE_FILE_ID')
sheets = {
    'students': 0,
    'skills': 1841603017,
    'words': 1927839585,
    'scores': [801859022]
}


def sync_students(gid: int) -> None:
    students_df = get_google_sheet_data(file_id=file_id, gid=gid)
    students_df.to_sql(
        name="student",
        con=engine,
        if_exists="replace",
        index=False,
    )


def sync_skills(gid: int) -> None:
    skills_df = get_google_sheet_data(file_id=file_id, gid=gid)
    skills_df.to_sql(
        name="skill",
        con=engine,
        if_exists="replace",
        index=False,
    )


def sync_words(gid: int) -> None:
    words_df = get_google_sheet_data(file_id=file_id, gid=gid)
    words_df.to_sql(
        name="word",
        con=engine,
        if_exists="replace",
        index=False,
    )


def sync_scores(gids: list[int]) -> None:
    dfs = []
    for gid in gids:
        df = get_google_sheet_data(file_id=file_id, gid=gid)
        dfs.append(df)

    id_vars = ['student_id', 'cycle']
    scores_df = (
        pd.concat(dfs)
        .rename(columns={'id': 'student_id'})
        .drop(columns=['grade', 'teacher', 'eld', 'name'])
        .pipe(lambda df: df.melt(
            id_vars=id_vars,
            value_vars=[c for c in df.columns if c not in id_vars],
            var_name="word",
            value_name="correct"
        ))
        .assign(correct=lambda df: df.correct.map({'PASS': 1, 'FAIL': 0}))
    )

    scores_df.to_sql(
        name="score",
        con=engine,
        if_exists="replace",
        index=False,
    )


def main():
    sync_students(gid=sheets['students'])
    sync_skills(gid=sheets['skills'])
    sync_words(gid=sheets['words'])
    sync_scores(gids=sheets['scores'])


if __name__ == '__main__':
    main()