package com.tishtech;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class UltimateMp3Encoder {

  private static final String FFMPEG_CMD = "ffmpeg";
  private static final String FFPROBE_CMD = "ffprobe";
  private static final String FILES_LIST_NAME = "./list.txt";
  private static final String RAW_PART_PREFIX = "raw_part_";
  private static final String CLEAN_PART_PREFIX = "clean_part_";
  private static final String MP3_EXTENSION = ".mp3";
  private static final String INPUT_FILE = "./input.wav";
  private static final String OUTPUT_FILE = "./output" + MP3_EXTENSION;
  private static final int THREADS = Runtime.getRuntime().availableProcessors();
  private static final double OVERLAP_SEC = 2.0;
  private static final double SAMPLE_RATE = 44100.0;
  private static final int SAMPLES_PER_FRAME = 1152;

  static void main() {
    try {
      long startTime = System.currentTimeMillis();
      IO.println("Start Ultimate pipeline. Threads: " + THREADS);
      double totalDurationSec = getAudioDuration();
      IO.println("Duration: " + totalDurationSec + " seconds");
      List<ChunkData> grid = calculateGrid(totalDurationSec);
      ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < grid.size(); i++) {
        final int index = i;
        final ChunkData chunk = grid.get(i);
        futures.add(executor.submit(() -> processChunk(index, chunk)));
      }
      List<String> cleanParts = new ArrayList<>();
      for (Future<String> f : futures) {
        cleanParts.add(f.get());
      }
      executor.shutdown();
      IO.println("Transcoding and cutting are done. Merging...");
      mergeParts(cleanParts);
      IO.println("Took: " + (System.currentTimeMillis() - startTime) + " ms");
      for (String part : cleanParts) Files.deleteIfExists(Paths.get(part));
      for (int i = 0; i < grid.size(); i++) {
        Files.deleteIfExists(Paths.get(RAW_PART_PREFIX + i + ".mp3"));
      }
      Files.deleteIfExists(Paths.get(FILES_LIST_NAME));
      IO.println("Done");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static String processChunk(int index, ChunkData chunk) throws Exception {
    String rawPart = RAW_PART_PREFIX + index + MP3_EXTENSION;
    String cleanPart = CLEAN_PART_PREFIX + index + MP3_EXTENSION;
    ProcessBuilder pbEncode = new ProcessBuilder(
      FFMPEG_CMD, "-y",
      "-ss", String.format(Locale.US, "%.9f", chunk.encodeStartSec),
      "-t", String.format(Locale.US, "%.9f", chunk.encodeDurationSec),
      "-i", INPUT_FILE,
      "-c:a", "libmp3lame",
      "-b:a", "320k",
      "-reservoir", "0",
      rawPart
    );
    runProcess(pbEncode);
    ProcessBuilder pbTrim = new ProcessBuilder(
      FFMPEG_CMD, "-y",
      "-i", rawPart,
      "-ss", String.format(Locale.US, "%.9f", chunk.trimStartSec),
      "-t", String.format(Locale.US, "%.9f", chunk.neededDurationSec),
      "-c", "copy",
      cleanPart
    );
    runProcess(pbTrim);
    return cleanPart;
  }

  private static List<ChunkData> calculateGrid(double totalDurationSec) {
    List<ChunkData> result = new ArrayList<>();
    long totalSamples = (long) (totalDurationSec * SAMPLE_RATE);
    long totalFrames = (long) Math.ceil((double) totalSamples / SAMPLES_PER_FRAME);
    long framesPerChunk = totalFrames / THREADS;
    long overlapFrames = Math.round((OVERLAP_SEC * SAMPLE_RATE) / SAMPLES_PER_FRAME);
    for (int i = 0; i < THREADS; i++) {
      ChunkData chunk = new ChunkData();
      long neededStartFrame = i * framesPerChunk;
      long neededEndFrame = (i == THREADS - 1) ? totalFrames : (i + 1) * framesPerChunk;
      chunk.neededFrames = neededEndFrame - neededStartFrame;
      chunk.neededDurationSec = (chunk.neededFrames * SAMPLES_PER_FRAME) / SAMPLE_RATE;
      long encodeStartFrame = Math.max(0, neededStartFrame - overlapFrames);
      long encodeEndFrame = Math.min(totalFrames, neededEndFrame + overlapFrames);
      long encodeFrames = encodeEndFrame - encodeStartFrame;
      chunk.encodeStartSec = (encodeStartFrame * SAMPLES_PER_FRAME) / SAMPLE_RATE;
      chunk.encodeDurationSec = (encodeFrames * SAMPLES_PER_FRAME) / SAMPLE_RATE;
      long trimOffsetFrames = neededStartFrame - encodeStartFrame;
      chunk.trimStartSec = (trimOffsetFrames * SAMPLES_PER_FRAME) / SAMPLE_RATE;
      result.add(chunk);
    }
    return result;
  }

  private static void mergeParts(List<String> parts) throws Exception {
    try (PrintWriter writer = new PrintWriter(FILES_LIST_NAME)) {
      for (String part : parts) writer.println("file '" + part + "'");
    }
    ProcessBuilder pbMerge = new ProcessBuilder(
      FFMPEG_CMD, "-y", "-f", "concat", "-safe", "0", "-i", FILES_LIST_NAME, "-c", "copy", OUTPUT_FILE
    );
    runProcess(pbMerge);
  }

  private static double getAudioDuration() throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
      FFPROBE_CMD, "-v", "error", "-show_entries", "format=duration",
      "-of", "default=noprint_wrappers=1:nokey=1", INPUT_FILE
    );
    Process p = pb.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line = reader.readLine();
      if (line != null) return Double.parseDouble(line);
    }
    throw new RuntimeException("Duration error");
  }

  private static void runProcess(ProcessBuilder pb) throws Exception {
    pb.redirectError(ProcessBuilder.Redirect.DISCARD);
    Process p = pb.start();
    if (p.waitFor() != 0) throw new RuntimeException("FFmpeg error");
  }

  private static class ChunkData {
    public double encodeStartSec, encodeDurationSec, trimStartSec, neededDurationSec;
    public long neededFrames;
  }
}
