import unittest

from pypika import Tables, Query
from pypika.functions import Now, IsNull
from pypika.enums import MergeMatchingSituation

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class MergeTest(unittest.TestCase):
    target, source = Tables("target", "source")

    def test_delete_on_unmatched_target_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .delete(when=MergeMatchingSituation.NOT_MATCHED_BY_SOURCE)
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id\"=\"target\".\"id\" "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )

    def test_conditioned_delete_on_unmatched_target_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .delete(
                when=MergeMatchingSituation.NOT_MATCHED_BY_SOURCE,
                and_=(self.target.expire_at >= Now()),
            )
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id\"=\"target\".\"id\" "
            "WHEN NOT MATCHED BY SOURCE AND (\"target\".\"expire_at\">NOW()) THEN DELETE",
        )

    def test_update_on_unmatched_target_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .update(when=MergeMatchingSituation.NOT_MATCHED_BY_SOURCE)
            .set(self.target.title, "(Expired Product)")
            .set(self.target.description, "(Not available now)")
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN NOT MATCHED BY SOURCE THEN UPDATE "
            "SET \"title\"='(Expired Product)',\"description\"='(Not available now)'",
        )

    def test_conditioned_update_on_unmatched_target_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .update(
                when=MergeMatchingSituation.NOT_MATCHED_BY_SOURCE,
                and_=(self.target.expire_at >= Now()),
            )
            .set(self.target.description, "(Not available now)")
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN NOT MATCHED BY SOURCE AND (\"target\".\"expire_at\">NOW()) THEN UPDATE"
            "SET \"description\"='(Not availablpe now)'",
        )

    def test_insert_on_unmatched_source_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .insert_unmatched_source(
                self.source.title,
                self.source.description,
                Now(),
            )
            .columns("title", "description", "updated_at")
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN NOT MATCHED THEN INSERT "
            "(\"title\",\"description\",\"updated_at\") "
            "VALUES (\"source\".\"title\",\"source\".\"description\",NOW())",
        )

    def test_conditioned_insert_on_unmatched_source_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .insert_unmatched_source(
                self.source.title,
                self.source.description,
                Now(),
                when=(IsNull(self.source.notes).negate()),
            )
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN NOT MATCHED AND NOT ISNULL(\"source\".\"notes\") THEN INSERT "
            "(\"title\",\"description\",\"updated_at\") "
            "VALUES (\"source\".\"title\",\"source\".\"description\",NOW())",
        )

    def test_delete_on_matched_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .delete(when=MergeMatchingSituation.MATCHED)
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN MATCHED THEN DELETE"
        )

    def test_conditioned_delete_on_matched_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .delete(
                when=MergeMatchingSituation.MATCHED,
                and_=(self.target.expire_at <= Now()),
            )
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN MATCHED AND \"target\".\"expire_at\"<=NOW() THEN DELETE"
        )

    def test_update_on_matched_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .update(when=MergeMatchingSituation.MATCHED)
            .set(self.target.title, self.source.title)
            .set(self.target.description, self.source.description)
            .set(self.target.updated_at, Now())
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN MATCHED THEN UPDATE "
            "SET "
            "\"target\".\"title\"=\"source\".\"title\","
            "\"target\".\"description\"=\"source\".\"description\","
            "\"target\".\"updated_at\"=NOW()",
        )

    def test_conditioned_update_on_matched_data(self):
        q = (
            Query
            .merge(self.target)
            .using(self.source)
            .on(self.source.id == self.target.id)
            .update(
                when=MergeMatchingSituation.MATCHED,
                and_=(self.source.expire_at < Now())
            )
            .set(self.target.title, self.source.title)
            .set(self.target.description, self.source.description)
            .set(self.target.updated_at, Now())
        )
        self.assertEqual(
            str(q),
            "MERGE \"target\" ON \"source\" WHERE \"source\".\"id=\"target\".\"id\" "
            "WHEN MATCHED AND \"target\".\"expire_at\"<=NOW() THEN UPDATE "
            "SET "
            "\"target\".\"title\"=\"source\".\"title\","
            "\"target\".\"description\"=\"source\".\"description\","
            "\"target\".\"updated_at\"=NOW()",
        )

    def test_combined_merge(self):
        pass
