from coordinator import split_text


def test_empty_input_gives_no_chunks():
    assert split_text("", 3) == []
    assert split_text("\n\n  \n", 3) == []


def test_fewer_paragraphs_than_chunks():
    chunks = split_text("one\n\ntwo", 5)
    assert chunks == [("chunk-1", "one"), ("chunk-2", "two")]


def test_order_preserved_and_nothing_lost():
    paragraphs = [f"paragraph {i}" for i in range(7)]
    chunks = split_text("\n\n".join(paragraphs), 3)
    assert len(chunks) == 3
    rejoined = "\n\n".join(text for _, text in chunks)
    assert rejoined == "\n\n".join(paragraphs)
    assert [cid for cid, _ in chunks] == ["chunk-1", "chunk-2", "chunk-3"]


def test_sizes_differ_by_at_most_one():
    chunks = split_text("\n\n".join(str(i) for i in range(4)), 3)
    sizes = [text.count("\n\n") + 1 for _, text in chunks]
    assert sorted(sizes) == [1, 1, 2]


def test_single_newlines_inside_a_paragraph_are_kept():
    chunks = split_text("line a\nline b\n\nline c", 1)
    assert chunks == [("chunk-1", "line a\nline b\n\nline c")]


def test_runs_of_blank_lines_collapse_to_one():
    chunks = split_text("a\n\n\n\nb", 1)
    assert chunks == [("chunk-1", "a\n\nb")]


def test_zero_or_negative_chunks_treated_as_one():
    assert len(split_text("a\n\nb", 0)) == 1
