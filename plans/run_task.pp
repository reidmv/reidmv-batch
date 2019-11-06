plan batch::run_task (
  TargetSpec $nodes,
  String     $task,
  Hash       $parameters = { },
  String     $results_directory,
  Integer    $batch_size = 25,
) {
  $targets = get_targets($nodes)

  $steps = Integer(($targets.size - 1) / $batch_size + 1)

  $reduction = $steps.reduce({'ok-count' => 0, 'error-count' => 0}) |$memo,$i| {
    $start = $i * $batch_size
    $batch_targets = $targets[$start, $batch_size]

    $batch_results = run_task($task, $batch_targets, $parameters)

    $batch_results.each |$result| {
      file::write("${results_directory}/${result.target.name.regsubst(/[\/: ]+/, '-')}.result.json", $result.to_data.to_json)
    }

    # Keep tabs of ok/error counts
    ({
      'ok-count'    => ($memo['ok-count'] + $batch_results.ok_set.count),
      'error-count' => ($memo['error-count'] + $batch_results.error_set.count),
    })
  }

  return($reduction)
}
