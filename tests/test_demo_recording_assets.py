import unittest

from demo.recording.common import build_subtitle_entries, load_config, timestamp_srt


class DemoRecordingAssetsTests(unittest.TestCase):
    def test_config_has_expected_scenes(self) -> None:
        config = load_config()
        self.assertEqual([scene.id for scene in config.scenes], [
            "opening",
            "data_source",
            "question_entry",
            "analysis_depth",
            "follow_up",
            "reuse",
        ])
        self.assertTrue(config.question)
        self.assertTrue(config.follow_up_question)

    def test_build_subtitle_entries_clamps_to_scene_end(self) -> None:
        config = load_config()
        timeline = {
            "scenes": [
                {"id": scene.id, "start_s": index * 10.0, "end_s": index * 10.0 + 4.0}
                for index, scene in enumerate(config.scenes)
            ]
        }
        entries = build_subtitle_entries(config, timeline)
        self.assertGreater(len(entries), 0)
        for entry in entries:
            self.assertLess(entry["start_s"], entry["end_s"])

    def test_timestamp_srt_format(self) -> None:
        self.assertEqual(timestamp_srt(65.432), "00:01:05,432")


if __name__ == "__main__":
    unittest.main()
