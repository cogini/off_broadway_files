defmodule ProducerTest do
  use ExUnit.Case

  alias OffBroadwayFiles.Producer

  describe "dispatch_events/3" do
    test "empty queue returns empty list of events" do
      queue = :queue.new()
      demand = 1

      assert {[], queue, demand} == Producer.dispatch_events(queue, :queue.len(queue), demand)
    end

    test "returns events when queue has enough to satisfy demand" do
      queue = :queue.from_list([1, 2, 3])
      demand = 2

      assert {events, new_queue, new_demand} = Producer.dispatch_events(queue, :queue.len(queue), demand)

      assert events == [1, 2]
      assert :queue.len(new_queue) == 1
      assert new_demand == 0
    end

    test "returns events when queue is equal to demand" do
      queue = :queue.from_list([1, 2, 3])
      demand = 3

      assert {events, new_queue, new_demand} = Producer.dispatch_events(queue, :queue.len(queue), demand)

      assert events == [1, 2, 3]
      assert :queue.len(new_queue) == 0
      assert new_demand == 0
    end

    test "returns events when queue is less than demand" do
      queue = :queue.from_list([1, 2, 3])
      demand = 4

      assert {events, new_queue, new_demand} = Producer.dispatch_events(queue, :queue.len(queue), demand)

      assert events == [1, 2, 3]
      assert :queue.len(new_queue) == 0
      assert new_demand == 1
    end
  end

  describe "match_names/2" do
    test "returns file when name matches pattern" do
      file_pattern = Regex.compile!(".*\\.log$")
      files = ["test.log", "example.log", "sample.txt"]
      assert Producer.match_names(files, file_pattern) == ["test.log", "example.log"]
    end

    test "returns empty list when input is empty" do
      file_pattern = Regex.compile!(".*\\.log$")
      files = []
      assert Producer.match_names(files, file_pattern) == []
    end
  end
end
