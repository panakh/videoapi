// n8n Code Node: Generate FFmpeg Command (Replicating server.js logic)

// --- Helper Functions (Copied/Adapted from server.js) ---

function formatAssTime(seconds) {
  // Use the precise version from server.js
  const hh = Math.floor(seconds / 3600);
  const mm = Math.floor((seconds % 3600) / 60);
  const ss = Math.floor(seconds % 60);
  const cs = Math.round((seconds - Math.floor(seconds)) * 100); // Use Math.round for centiseconds
  return `${hh}:${String(mm).padStart(2, "0")}:${String(ss).padStart(
    2,
    "0"
  )}.${String(cs).padStart(2, "0")}`;
}

function generateAssSubtitles(transcript, style = {}) {
  // Use the precise version from server.js
  const defaultStyle = {
    Name: "Default",
    Fontname: "Arial",
    Fontsize: "28",
    PrimaryColour: "&H00FFFFFF",
    SecondaryColour: "&H000000FF",
    OutlineColour: "&H00000000",
    BackColour: "&H80000000",
    Bold: "0",
    Italic: "0",
    Underline: "0",
    StrikeOut: "0",
    ScaleX: "100",
    ScaleY: "100",
    Spacing: "0",
    Angle: "0",
    BorderStyle: "1",
    Outline: "1.5",
    Shadow: "1",
    Alignment: "2",
    MarginL: "10",
    MarginR: "10",
    MarginV: "20",
    Encoding: "1",
  };
  const mergedStyle = { ...defaultStyle, ...style };
  let assContent = `[Script Info]
Title: Generated Subtitles
ScriptType: v4.00+
WrapStyle: 0
ScaledBorderAndShadow: yes
YCbCr Matrix: None
PlayResX: 1920
PlayResY: 1080

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: ${mergedStyle.Name},${mergedStyle.Fontname},${mergedStyle.Fontsize},${mergedStyle.PrimaryColour},${mergedStyle.SecondaryColour},${mergedStyle.OutlineColour},${mergedStyle.BackColour},${mergedStyle.Bold},${mergedStyle.Italic},${mergedStyle.Underline},${mergedStyle.StrikeOut},${mergedStyle.ScaleX},${mergedStyle.ScaleY},${mergedStyle.Spacing},${mergedStyle.Angle},${mergedStyle.BorderStyle},${mergedStyle.Outline},${mergedStyle.Shadow},${mergedStyle.Alignment},${mergedStyle.MarginL},${mergedStyle.MarginR},${mergedStyle.MarginV},${mergedStyle.Encoding}

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
`;
  for (let i = 0; i < transcript.length; i++) {
    const wordInfo = transcript[i];
    const start = wordInfo.start;
    const end = wordInfo.end;
    const word = wordInfo.word;
    const duration = end - start;
    if (duration <= 0) {
      console.warn(
        `Skipping word "${word}" at index ${i} due to non-positive duration (${duration.toFixed(
          3
        )}s)`
      );
      continue;
    }
    assContent += `Dialogue: 0,${formatAssTime(start)},${formatAssTime(end)},${
      mergedStyle.Name
    },,0,0,0,,${word}\n`;
  }
  return assContent;
}

function normalizeText(text) {
  // Copied from server.js
  if (!text) return "";
  return text
    .toLowerCase()
    .replace(/[.,!?;:'"-]/g, "")
    .trim();
}

function findWordSequence(transcript, text) {
  // Copied from server.js
  if (!transcript || transcript.length === 0 || !text) {
    return null;
  }
  const normalizedText = normalizeText(text);
  const targetWords = normalizedText.split(/\s+/).filter((w) => w.length > 0);
  if (targetWords.length === 0) {
    return null;
  }
  const transcriptWords = transcript.map((item) => ({
    ...item,
    normalizedWord: normalizeText(item.word),
  }));
  for (let i = 0; i <= transcriptWords.length - targetWords.length; i++) {
    let match = true;
    for (let j = 0; j < targetWords.length; j++) {
      if (transcriptWords[i + j].normalizedWord !== targetWords[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      const startTime = transcriptWords[i].start;
      const endTime = transcriptWords[i + targetWords.length - 1].end;
      const duration = Math.max(0, endTime - startTime);
      return {
        startTime,
        endTime,
        duration,
        startIndex: i,
        endIndex: i + targetWords.length - 1,
      };
    }
  }
  console.warn(
    `Could not find sequence "${text}" (normalized: "${normalizedText}") in transcript.`
  );
  return null;
}

// Helper function to download a file (remains the same)
async function downloadFile(url, destPath) {
  const https = require("https");
  const fs = require("fs");
  const file = fs.createWriteStream(destPath);
  return new Promise((resolve, reject) => {
    const request = https.get(url, (response) => {
      if (response.statusCode !== 200) {
        file.close();
        fs.unlink(destPath, () => {});
        reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
        return;
      }
      response.pipe(file);
    });
    file.on("finish", () => {
      file.close(resolve);
    });
    request.on("error", (err) => {
      fs.unlink(destPath, () => {});
      reject(err);
    });
    file.on("error", (err) => {
      fs.unlink(destPath, () => {});
      reject(err);
    });
  });
}

// --- Main Async Function ---
async function main() {
  const newItem = {};
  const inputData = $input.item.json;

  // --- Basic Input Validation ---
  if (!inputData || typeof inputData !== "object") {
    throw new Error("Invalid input data received. Expected a JSON object.");
  }
  if (
    !inputData.segments ||
    !Array.isArray(inputData.segments) ||
    inputData.segments.length === 0
  ) {
    throw new Error(
      'Input data is missing required "segments" array or it is empty.'
    );
  }
  if (!inputData.audioInBase64 || typeof inputData.audioInBase64 !== "string") {
    throw new Error('Input data is missing required "audioInBase64" string.');
  }
  if (!inputData.transcript || !Array.isArray(inputData.transcript)) {
    console.warn(
      'Input data has missing or non-array "transcript". Subtitles might be empty.'
    );
    inputData.transcript = [];
  }

  // --- Node.js Modules ---
  const fsPromises = require("fs").promises;
  const path = require("path");
  const os = require("os");
  const crypto = require("crypto");
  const { URL } = require("url");

  // --- Setup Temp Dir ---
  const requestId = crypto.randomUUID();
  const baseTempDir = os.tmpdir();
  const tempDir = path.join(baseTempDir, "n8n_ffmpeg_" + requestId);
  newItem.json = { ...inputData }; // Copy input data early

  try {
    await fsPromises.mkdir(tempDir, { recursive: true });
    console.log(`Created temp directory: ${tempDir}`);

    // --- 1. Decode Audio ---
    const audioFilePath = path.join(tempDir, "audio.mp3");
    const audioBuffer = Buffer.from(inputData.audioInBase64, "base64");
    await fsPromises.writeFile(audioFilePath, audioBuffer);
    console.log(`Decoded audio saved to: ${audioFilePath}`);
    newItem.json.audioFilePath = audioFilePath;

    // --- Get Audio Duration ---
    let audioDuration;
    try {
      const lastWord =
        inputData.transcript.length > 0
          ? inputData.transcript[inputData.transcript.length - 1]
          : null;
      if (lastWord && typeof lastWord.end === "number") {
        audioDuration = lastWord.end;
        console.log(
          `Using audio duration from transcript end time: ${audioDuration} seconds`
        );
      } else {
        audioDuration = inputData.segments.reduce(
          (sum, s) => sum + (parseFloat(s.durationOnScreen) || 5.0),
          0
        );
        console.warn(
          `Estimating audio duration based on segment durations: ${audioDuration} seconds`
        );
      }
      console.log(`Audio duration (determined): ${audioDuration} seconds`);
      if (
        typeof audioDuration !== "number" ||
        isNaN(audioDuration) ||
        audioDuration <= 0
      ) {
        throw new Error(`Invalid audio duration calculated: ${audioDuration}`);
      }
    } catch (err) {
      console.error("Error determining audio duration:", err);
      throw new Error("Failed to determine a valid audio duration.");
    }

    // --- 2. Download Images ---
    console.log("Starting image downloads...");
    const downloadedImagePaths = [];
    const downloadPromises = inputData.segments.map(async (segment, index) => {
      if (
        !segment.images ||
        !Array.isArray(segment.images) ||
        segment.images.length === 0 ||
        !segment.images[0].url
      ) {
        throw new Error(
          `Segment ${index} is missing a valid image URL in segment.images[0].url`
        );
      }
      const imageUrl = segment.images[0].url;
      let extension = ".jpg";
      try {
        const parsedUrl = new URL(imageUrl);
        const ext = path.extname(parsedUrl.pathname);
        if (ext) {
          extension = ext;
        }
      } catch (urlError) {
        console.warn(
          `Could not parse URL to get extension for ${imageUrl}. Using default ${extension}. Error: ${urlError.message}`
        );
      }
      const imageFileName = `image_${index}${extension}`;
      const localImagePath = path.join(tempDir, imageFileName);
      console.log(
        `Downloading image ${index}: ${imageUrl} to ${localImagePath}`
      );
      await downloadFile(imageUrl, localImagePath);
      console.log(`Finished downloading image ${index}`);
      downloadedImagePaths[index] = localImagePath;
    });
    await Promise.all(downloadPromises);
    console.log("All images downloaded successfully.");
    newItem.json.imageLocalPaths = downloadedImagePaths;

    // --- 3. Calculate Timeline (Logic from server.js) ---
    const timedSegments = [];
    const defaultFixedDuration = 5.0;
    const minVisualDuration = 0.1; // Define minVisualDuration
    const transcript = inputData.transcript;
    const segments = inputData.segments;

    // --- Pass 1: Pre-calculate dynamic timings ---
    console.log("--- Pre-calculating Dynamic Timings ---");
    const dynamicTimings = segments.map((segment, i) => {
        const durationMode = (segment.durationMode || inputData.durationMode || 'fixed'); // Use inputData.durationMode
        if (durationMode === 'dynamic' && segment.textOnScreen) {
            const wordSequence = findWordSequence(transcript, segment.textOnScreen);
            if (wordSequence) {
                console.log(`Segment ${i} (dynamic-precalc): Found text. Start=${wordSequence.startTime.toFixed(3)}, End=${wordSequence.endTime.toFixed(3)}`);
                return {
                    dynamicSuccess: true,
                    startTime: wordSequence.startTime,
                    endTime: wordSequence.endTime,
                    speechDuration: wordSequence.duration
                };
            } else {
                console.warn(`Segment ${i} (dynamic-precalc): Could not find text \"${segment.textOnScreen}\".`);
                return { dynamicSuccess: false };
            }
        } else {
            return { dynamicSuccess: false }; // Mark as not dynamic or missing text
        }
    });

    // --- Pass 2: Calculate final timings sequentially ---
    console.log("--- Calculating Final Timings Sequentially ---");
    let videoTime = 0; // Tracks the end time of the last placed segment
    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i];
        const precalc = dynamicTimings[i];
        const durationMode = (segment.durationMode || inputData.durationMode || 'fixed'); // Use inputData.durationMode
        
        let segmentTiming = {
            display_start_time: 0,
            display_end_time: 0,
            effective_visual_duration: 0,
            speech_duration: 0,
            index: i
        };

        if (precalc.dynamicSuccess) {
            segmentTiming.display_start_time = Math.max(videoTime, precalc.startTime);
            // Ensure end time is at least minVisualDuration after the start, and respects dynamic end
            segmentTiming.display_end_time = Math.max(segmentTiming.display_start_time + minVisualDuration, precalc.endTime);
            segmentTiming.speech_duration = precalc.speechDuration;
            console.log(`Segment ${i} (dynamic-placed): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}`);
        } else { // Fixed mode or dynamic fallback
            const fixedDuration = parseFloat(segment.durationOnScreen) || defaultFixedDuration;
            segmentTiming.display_start_time = videoTime;
            segmentTiming.display_end_time = videoTime + fixedDuration;
            segmentTiming.speech_duration = fixedDuration;
            console.log(`Segment ${i} (${durationMode}-placed): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}`);
        }

        segmentTiming.effective_visual_duration = Math.max(minVisualDuration, segmentTiming.display_end_time - segmentTiming.display_start_time);
        // Ensure end time is correctly set after visual duration is potentially clamped by minVisualDuration
        segmentTiming.display_end_time = segmentTiming.display_start_time + segmentTiming.effective_visual_duration;

        // --- Adjust LAST segment --- 
        if (i === segments.length - 1) {
             // Clamp the end time to the audio duration first
             segmentTiming.display_end_time = Math.min(segmentTiming.display_end_time, audioDuration);
             // Extend the end time if it's shorter than the audio
            if (segmentTiming.display_end_time < audioDuration) {
                 console.log(`Adjusting last segment end time from ${segmentTiming.display_end_time.toFixed(3)} to ${audioDuration.toFixed(3)}`);
                 segmentTiming.display_end_time = audioDuration;
             }
             // Recalculate final visual duration
             segmentTiming.effective_visual_duration = Math.max(minVisualDuration, segmentTiming.display_end_time - segmentTiming.display_start_time);
             // Update speech duration if it wasn't dynamically set
             if (!precalc.dynamicSuccess) {
                 segmentTiming.speech_duration = segmentTiming.effective_visual_duration;
             }
        }

        timedSegments.push(segmentTiming);
        videoTime = segmentTiming.display_end_time; // Update video time for the next iteration
        console.log(`Segment ${i} (Final): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}, VisualDur=${segmentTiming.effective_visual_duration.toFixed(3)}, SpeechDur=${segmentTiming.speech_duration.toFixed(3)}`);
    }
    // Overlap correction loop removed as the new sequential placement logic should handle this better.
    newItem.json.timedSegments = timedSegments;

    // --- 4. Generate Subtitles ---
    const subtitlesPath = path.join(tempDir, "subtitles.ass");
    let subtitlesExist = false;
    if (inputData.transcript.length > 0) {
      try {
        const assContent = generateAssSubtitles(transcript);
        await fsPromises.writeFile(subtitlesPath, assContent);
        console.log(`Generated ASS subtitles: ${subtitlesPath}`);
        newItem.json.subtitlesFilePath = subtitlesPath;
        subtitlesExist = true;
      } catch (subError) {
        console.error("Error generating subtitles:", subError);
        console.warn("Proceeding without subtitles due to generation error.");
        newItem.json.subtitlesFilePath = null;
      }
    } else {
      console.log("Transcript is empty, skipping subtitle generation.");
      newItem.json.subtitlesFilePath = null;
    }

    // --- 5. Construct FFmpeg Command Arguments ---
    const outputVideoPath = path.join(tempDir, "output.mp4");
    const imagePaths = downloadedImagePaths;
    const outputWidth = 1920;
    const outputHeight = 1080;
    const fadeDuration = 0.5;
    const fps = 60;

    const ffmpegArgs = []; // This will hold the arguments array
    const inputs = [];
    const filterComplexParts = [];

    // --- Inputs ---
    imagePaths.forEach((imgPath) => {
      inputs.push("-i", imgPath);
    });
    inputs.push("-i", audioFilePath);
    ffmpegArgs.push(...inputs);

    // --- Filter Complex ---
    const baseCanvasTag = "base";
    // Using original color filter definition
    filterComplexParts.push(
      `color=black:s=${outputWidth}x${outputHeight}:d=${audioDuration}[${baseCanvasTag}]`
    );
    let lastOverlayOutput = baseCanvasTag;

    timedSegments.forEach((segment, index) => {
      const imgInputIndex = index;
      const start = segment.display_start_time;
      const visualDuration = segment.effective_visual_duration;
      const speechDuration = segment.speech_duration;
      const zoompanTag = `zoompan${index}`;
      const formatTag = `format${index}`;
      const finalSegmentVideoTag = `v${index}`;

      const zoompanDurationSeconds = Math.max(0.1, speechDuration);
      const zoompanDurationFrames = Math.ceil(zoompanDurationSeconds * fps);

      let zoompanX, zoompanY;
      if (index % 2 === 0) {
        zoompanX = "0*(zoom-1)";
        zoompanY = "ih-ih/zoom";
      } else {
        zoompanX = "iw-iw/zoom";
        zoompanY = "0";
      }

      const zoomExpression = "if(eq(on,1),1,min(zoom+0.0010,1.5))";
      const zoompanFilter = `zoompan=z='${zoomExpression}':x='${zoompanX}':y='${zoompanY}':d=${zoompanDurationFrames}:s=${outputWidth}x${outputHeight}:fps=${fps}`;

      filterComplexParts.push(
        `[${imgInputIndex}:v]${zoompanFilter}[${zoompanTag}]`,
        `[${zoompanTag}]format=pix_fmts=yuva420p[${formatTag}]`
      );

      filterComplexParts.push(
        `[${formatTag}]setpts=PTS-STARTPTS+${start}/TB[${finalSegmentVideoTag}]`
      );

      const currentOverlayOutput = `ovl${index}`;
      const overlayFormat =
        index === timedSegments.length - 1 ? ":format=yuv420" : "";
      filterComplexParts.push(
        `[${lastOverlayOutput}][${finalSegmentVideoTag}]overlay=shortest=0:x=0:y=0${overlayFormat}[${currentOverlayOutput}]`
      );
      lastOverlayOutput = currentOverlayOutput;
    });

    // --- Subtitles ---
    let finalVideoOutputTag = lastOverlayOutput;
    if (subtitlesExist) {
      const subtitlesOutputTag = "subtitled";
      const escapedSubsPath = subtitlesPath
        .replace(/\\/g, "/")
        .replace(/:/g, "\\:");
      filterComplexParts.push(
        `[${lastOverlayOutput}]ass=filename='${escapedSubsPath}'[${subtitlesOutputTag}]`
      );
      finalVideoOutputTag = subtitlesOutputTag;
    }

    // Add the filter_complex argument
    const finalFilterComplexString = filterComplexParts.join(";");
    ffmpegArgs.push("-filter_complex", finalFilterComplexString); // Add as separate argument

    // --- Output Mapping and Options ---
    ffmpegArgs.push(
      "-map",
      `[${finalVideoOutputTag}]`,
      "-map",
      `${imagePaths.length}:a`,
      "-c:v",
      "libx264",
      "-preset",
      "fast",
      "-crf",
      "23",
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      "-pix_fmt",
      "yuv420p",
      "-movflags",
      "+faststart",
      "-y",
      outputVideoPath
    );

    // **MODIFIED: Construct the final command string again**
    const commandString =
      "ffmpeg " +
      ffmpegArgs
        .map((arg) => {
          // Use robust single-quoting for shell compatibility
          if (typeof arg === "string") {
            return `'${arg.replace(/'/g, "'\\''")}'`; // POSIX-compliant escaping
          }
          return arg;
        })
        .join(" ");

    console.log("Generated FFmpeg command:", commandString);
    newItem.json.ffmpegCommand = commandString; // **Output the single command string**

    // Keep other output paths for reference/cleanup
    newItem.json.outputVideoPath = outputVideoPath;
    newItem.json.tempDir = tempDir;
  } catch (error) {
    console.error("Error in n8n Code node:", error);
    newItem.error = `FFmpeg Command Generation Failed: ${error.message}`;
    newItem.errorStack = error.stack;
  }

  return newItem;
}

// Call the main async function and return its promise
return main();
