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

// Helper function to format a command and its arguments for shell execution
function formatCommandForShell(commandName, argsArray) {
  const formattedArgs = argsArray.map(arg => {
    if (typeof arg === 'number') {
      return arg.toString(); // Numbers are fine as is
    }
    if (typeof arg !== 'string') {
      // This case should ideally not happen if args are prepared correctly
      // but if it does, return an empty string or handle as an error.
      console.warn(`formatCommandForShell encountered non-string/non-number arg: ${arg}`);
      return ''; 
    }
    // For string arguments, wrap in double quotes.
    // Escape characters that are special within double quotes for many shells: backslash, double quote, dollar sign.
    // Add other characters like ` (backtick) if needed, depending on shell and context.
    const escapedArg = arg.replace(/[\\"$`]/g, '\\$&'); // Escape \, ", $, `
    return `"${escapedArg}"`;
  });
  return `${commandName} ${formattedArgs.join(' ')}`;
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
  const fs = require('fs'); // <--- Added fs module

  // --- Configuration for Chunking ---
  const CHUNK_SIZE = 5; // Number of segments per chunk
  const USE_CHUNKING = true; // Always true for this node

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
    newItem.json.audioFilePath = audioFilePath; // Keep for reference

    // --- Get Audio Duration ---
    let audioDuration;
    try {
      // Prefer ffprobe-like accurate duration if available, otherwise estimate
      // For n8n, we'll rely on transcript or sum of durations.
      // The server.js uses ffprobe, which is more accurate.
      // This node will use the existing estimation method.
      const lastWord =
        inputData.transcript.length > 0
          ? inputData.transcript[inputData.transcript.length - 1]
          : null;
      if (lastWord && typeof lastWord.end === "number" && lastWord.end > 0) {
        audioDuration = lastWord.end;
        console.log(
          `Using audio duration from transcript end time: ${audioDuration} seconds`
        );
      } else {
        // Fallback: estimate from segment durations (less accurate but necessary if transcript is missing/bad)
        let calculatedSum = inputData.segments.reduce(
            (sum, s) => sum + (parseFloat(s.durationOnScreen) || 5.0), 0
        );
        if (calculatedSum <= 0) { // Ensure a positive duration
            console.warn("Calculated sum of segment durations is non-positive. Using default 10s.");
            calculatedSum = 10.0;
        }
        audioDuration = calculatedSum;
        console.warn(
          `Estimating audio duration based on segment durations: ${audioDuration} seconds`
        );
      }
      if (
        typeof audioDuration !== "number" ||
        isNaN(audioDuration) ||
        audioDuration <= 0
      ) {
        console.error(`Invalid audio duration calculated (${audioDuration}), defaulting to 10s.`);
        audioDuration = 10.0; // Default to a sensible value if calculation fails
      }
       console.log(`Audio duration (determined for timeline): ${audioDuration} seconds`);
    } catch (err) {
      console.error("Error determining audio duration:", err);
      throw new Error("Failed to determine a valid audio duration.");
    }

    // --- 2. Download Images ---
    console.log("Starting image downloads...");
    const downloadedImagePaths = []; // This will store absolute paths
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
      let extension = ".jpg"; // Default extension
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
      const localImagePath = path.join(tempDir, imageFileName); // Absolute path
      console.log(
        `Downloading image ${index}: ${imageUrl} to ${localImagePath}`
      );
      await downloadFile(imageUrl, localImagePath);
      console.log(`Finished downloading image ${index}`);
      downloadedImagePaths[index] = localImagePath; // Store absolute path
    });
    await Promise.all(downloadPromises);
    console.log("All images downloaded successfully.");
    // newItem.json.imageLocalPaths = downloadedImagePaths; // Will be part of ffmpegProcessingDetails

    // --- 3. Calculate Timeline (Logic from server.js, durationMode always 'dynamic') ---
    const timedSegments = [];
    const defaultFixedDuration = 5.0; // Fallback for a segment if dynamic fails
    const minVisualDuration = 0.1;
    const transcript = inputData.transcript; // Already validated
    const segments = inputData.segments;     // Already validated
    const outputWidth = inputData.outputWidth || 1920;
    const outputHeight = inputData.outputHeight || 1080;
    const fps = inputData.fps || 60;


    // --- Pass 1: Pre-calculate dynamic timings (durationMode is always 'dynamic') ---
    console.log("--- Pre-calculating Dynamic Timings (mode: dynamic) ---");
    const dynamicTimings = segments.map((segment, i) => {
        // const durationMode = (segment.durationMode || inputData.durationMode || 'fixed'); // Use inputData.durationMode
        // For this node, durationMode is considered 'dynamic'
        if (segment.textOnScreen) { // textOnScreen is required for dynamic
            const wordSequence = findWordSequence(transcript, segment.textOnScreen);
            if (wordSequence) {
                console.log(`Segment ${i} (dynamic-precalc): Found text. Start=${wordSequence.startTime.toFixed(3)}, End=${wordSequence.endTime.toFixed(3)}`);
                return {
                    dynamicSuccess: true,
                    startTime: wordSequence.startTime,
                    endTime: wordSequence.endTime,
                    speechDuration: wordSequence.duration
                };
            } else { // Dynamic mode failed for this segment
                console.warn(`Segment ${i} (dynamic-precalc): Could not find text \"${segment.textOnScreen}\". Will use fixed fallback.`);
                return { dynamicSuccess: false, speechDuration: defaultFixedDuration }; // Fallback duration
            }
        } else { // No textOnScreen, cannot be dynamic
             console.warn(`Segment ${i} (dynamic-precalc): No textOnScreen provided. Will use fixed fallback.`);
            return { dynamicSuccess: false, speechDuration: defaultFixedDuration }; // Fallback duration
        }
    });

    // --- Pass 2: Calculate final timings sequentially ---
    console.log("--- Calculating Final Timings Sequentially ---");
    let videoTime = 0; // Tracks the end time of the last placed segment
    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i];
        const precalc = dynamicTimings[i];
        // const durationMode = (segment.durationMode || inputData.durationMode || 'fixed');
        // Duration mode is treated as dynamic with fallback here.
        
        let segmentTiming = {
            display_start_time: 0,
            display_end_time: 0,
            effective_visual_duration: 0,
            speech_duration: 0, // Will be set based on precalc or fallback
            index: i
        };

        if (precalc.dynamicSuccess) {
            segmentTiming.display_start_time = Math.max(videoTime, precalc.startTime);
            segmentTiming.display_end_time = Math.max(segmentTiming.display_start_time + minVisualDuration, precalc.endTime);
            segmentTiming.speech_duration = precalc.speechDuration;
            // console.log(`Segment ${i} (dynamic-placed): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}`);
        } else { // Fallback for this segment (dynamic failed or no text)
            const fixedDuration = precalc.speechDuration; // Use the fallback duration from precalc
            segmentTiming.display_start_time = videoTime;
            segmentTiming.display_end_time = videoTime + fixedDuration;
            segmentTiming.speech_duration = fixedDuration;
            // console.log(`Segment ${i} (fixed-fallback-placed): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}`);
        }

        segmentTiming.effective_visual_duration = Math.max(minVisualDuration, segmentTiming.display_end_time - segmentTiming.display_start_time);
        segmentTiming.display_end_time = segmentTiming.display_start_time + segmentTiming.effective_visual_duration;

        if (i === segments.length - 1) { // Adjust LAST segment
             segmentTiming.display_end_time = Math.min(segmentTiming.display_end_time, audioDuration);
            if (segmentTiming.display_end_time < audioDuration) {
                 console.log(`Adjusting last segment end time from ${segmentTiming.display_end_time.toFixed(3)} to ${audioDuration.toFixed(3)}`);
                 segmentTiming.display_end_time = audioDuration;
             }
             segmentTiming.effective_visual_duration = Math.max(minVisualDuration, segmentTiming.display_end_time - segmentTiming.display_start_time);
             if (!precalc.dynamicSuccess) { // If it was a fallback, ensure speech_duration matches visual
                 segmentTiming.speech_duration = segmentTiming.effective_visual_duration;
             }
        }

        timedSegments.push(segmentTiming);
        videoTime = segmentTiming.display_end_time;
        console.log(`Segment ${i} (Final): Start=${segmentTiming.display_start_time.toFixed(3)}, End=${segmentTiming.display_end_time.toFixed(3)}, VisualDur=${segmentTiming.effective_visual_duration.toFixed(3)}, SpeechDur=${segmentTiming.speech_duration.toFixed(3)}`);
    }
    // newItem.json.timedSegments = timedSegments; // Will be part of ffmpegProcessingDetails

    // --- 4. Generate Subtitles ---
    const subtitlesFileName = "subtitles.ass";
    const subtitlesPath = path.join(tempDir, subtitlesFileName); // Absolute path
    let subtitlesExist = false;
    if (inputData.transcript && inputData.transcript.length > 0) {
      try {
        const assContent = generateAssSubtitles(transcript); // transcript is already defined
        await fsPromises.writeFile(subtitlesPath, assContent);
        console.log(`Generated ASS subtitles: ${subtitlesPath}`);
        // newItem.json.subtitlesFilePath = subtitlesPath; // Part of details
        subtitlesExist = true;
      } catch (subError) {
        console.error("Error generating subtitles:", subError);
        console.warn("Proceeding without subtitles due to generation error.");
        // newItem.json.subtitlesFilePath = null;
      }
    } else {
      console.log("Transcript is empty or not provided, skipping subtitle generation.");
      // newItem.json.subtitlesFilePath = null;
    }

    // --- 5. Collect Segment Parameters for Chunking ---
    // (Adapted from server.js constructFfmpegCommand)
    const collectedSegmentParameters = [];
    timedSegments.forEach((segment, index) => {
      const speechDuration = segment.speech_duration;
      const zoompanDurationSeconds = Math.max(0.1, speechDuration);
      const zoompanDurationFrames = Math.ceil(zoompanDurationSeconds * fps);

      let rawZoompanX, rawZoompanY;
      // Simple alternating zoom for variety, can be made more complex or configurable
      if (index % 2 === 0) { 
          rawZoompanX = '0*(zoom-1)'; 
          rawZoompanY = '0*(zoom-1)'; 
      } else {
          rawZoompanX = 'iw-iw/zoom'; 
          rawZoompanY = 'ih-ih/zoom'; 
      }
      // A common zoom expression: slowly zoom in up to 1.5x
      const rawZoomExpression = 'if(eq(on,1),1,min(zoom+0.0010,1.5))';

      collectedSegmentParameters.push({
          originalIndex: index,
          imagePath: downloadedImagePaths[index], // Absolute path
          // Store raw expressions; shell escaping will be handled by formatCommandForShell
          zoomExpression: rawZoomExpression,
          zoompanX: rawZoompanX,
          zoompanY: rawZoompanY,
          zoompanDurationFrames: zoompanDurationFrames,
          outputWidth: outputWidth,
          outputHeight: outputHeight,
          fps: fps,
          displayStartTimeGlobal: segment.display_start_time,
          effectiveVisualDurationGlobal: segment.effective_visual_duration,
          speechDurationGlobal: segment.speech_duration,
      });
    });

    // --- 6. Generate FFmpeg Chunk Commands ---
    const ffmpegChunkCommands = [];
    const chunkOutputFilePaths = []; // Relative paths for concat list, absolute for command

    if (!collectedSegmentParameters || collectedSegmentParameters.length === 0) {
        throw new Error("No segment parameters collected for chunk processing.");
    }
    
    for (let i = 0; i < collectedSegmentParameters.length; i += CHUNK_SIZE) {
        const batch = collectedSegmentParameters.slice(i, i + CHUNK_SIZE);
        if (batch.length === 0) continue;

        const chunkIndex = Math.floor(i / CHUNK_SIZE);
        const chunkOutputFileName = `video_chunk_${chunkIndex}.mp4`;
        const chunkOutputPath = path.join(tempDir, chunkOutputFileName); // Absolute path
        console.log(`Preparing chunk ${chunkIndex}... Output: ${chunkOutputPath}`);

        const chunkImageInputs = [];
        const chunkFilterComplexParts = [];
        
        const firstSegmentOriginalIndex = batch[0].originalIndex;
        const lastSegmentOriginalIndex = batch[batch.length - 1].originalIndex;

        if (timedSegments[firstSegmentOriginalIndex] === undefined || timedSegments[lastSegmentOriginalIndex] === undefined) {
            throw new Error(`Invalid segment index for chunk ${chunkIndex}. Cannot determine chunk start/end times from timedSegments.`);
        }

        const chunkGlobalStartTime = timedSegments[firstSegmentOriginalIndex].display_start_time;
        let chunkGlobalEndTime;

        let nextSegmentOriginalIndex = -1;
        // Check if there's a segment that would start the *next* batch
        if (i + CHUNK_SIZE < collectedSegmentParameters.length) {
            nextSegmentOriginalIndex = collectedSegmentParameters[i + CHUNK_SIZE].originalIndex;
        } 
        // If not a full next batch, check if there's any segment *immediately after* the current batch's last segment.
        // This covers the case where the last few segments don't form a full CHUNK_SIZE batch.
        // We look directly into timedSegments for this, assuming originalIndex corresponds to timedSegments index.
        else if (lastSegmentOriginalIndex + 1 < timedSegments.length) { 
            nextSegmentOriginalIndex = lastSegmentOriginalIndex + 1; 
        }

        if (nextSegmentOriginalIndex !== -1 && timedSegments[nextSegmentOriginalIndex]) {
            chunkGlobalEndTime = timedSegments[nextSegmentOriginalIndex].display_start_time;
            console.log(`Chunk ${chunkIndex}: Ends at start of next segment (${nextSegmentOriginalIndex}) @ ${chunkGlobalEndTime.toFixed(3)}s (Preserving silence)`);
      } else {
            chunkGlobalEndTime = audioDuration; // Last chunk, extend to full audio duration
            console.log(`Chunk ${chunkIndex}: Last chunk or no clear next segment, extending to audio_duration: ${chunkGlobalEndTime.toFixed(3)}s`);
        }
        
        // Final clamping and validation for chunkGlobalEndTime
        chunkGlobalEndTime = Math.max(chunkGlobalEndTime, timedSegments[lastSegmentOriginalIndex].display_end_time); // Must be at least the end of its last visual segment
        chunkGlobalEndTime = Math.max(chunkGlobalStartTime + minVisualDuration, chunkGlobalEndTime); // Ensure minimal positive duration
        chunkGlobalEndTime = Math.min(chunkGlobalEndTime, audioDuration); // Cap at total audio duration

        const chunkVideoDuration = Math.max(minVisualDuration, chunkGlobalEndTime - chunkGlobalStartTime);
        
        if (isNaN(chunkVideoDuration) || isNaN(chunkGlobalStartTime) || isNaN(chunkGlobalEndTime) || chunkVideoDuration <=0) {
             throw new Error(`Calculated timing is NaN or non-positive for chunk ${chunkIndex}. Start: ${chunkGlobalStartTime}, End: ${chunkGlobalEndTime}, VidDur: ${chunkVideoDuration}`);
      }

        batch.forEach((segmentParams) => { // segmentParams are from collectedSegmentParameters
            chunkImageInputs.push('-i', segmentParams.imagePath); // Absolute image paths
        });
        
        const audioInputIndex = chunkImageInputs.length / 2; 
        chunkImageInputs.push('-ss', chunkGlobalStartTime.toFixed(6));
        chunkImageInputs.push('-to', chunkGlobalEndTime.toFixed(6));
        chunkImageInputs.push('-i', audioFilePath); // Absolute audio path

        chunkFilterComplexParts.push(`color=black:s=${outputWidth}x${outputHeight}:d=${chunkVideoDuration.toFixed(6)}[base_chunk_${chunkIndex}]`);
        let lastChunkOverlayOutput = `base_chunk_${chunkIndex}`;

        batch.forEach((segmentParams, batchSegmentIndex) => {
            const imgInputIndexInChunk = batchSegmentIndex; 
            const segmentStartTimeInChunk = Math.max(0, segmentParams.displayStartTimeGlobal - chunkGlobalStartTime);
            
            const zoompanTag = `czp${chunkIndex}_${batchSegmentIndex}`;
            const formatTag = `cfmt${chunkIndex}_${batchSegmentIndex}`;
            const finalSegmentVideoTag = `cv${chunkIndex}_${batchSegmentIndex}`;

            const zoompanFilter = `zoompan=z='${segmentParams.zoomExpression}':x='${segmentParams.zoompanX}':y='${segmentParams.zoompanY}':d=${segmentParams.zoompanDurationFrames}:s=${segmentParams.outputWidth}x${segmentParams.outputHeight}:fps=${segmentParams.fps}`;

            chunkFilterComplexParts.push(
                `[${imgInputIndexInChunk}:v]${zoompanFilter}[${zoompanTag}]`,
                `[${zoompanTag}]format=pix_fmts=yuva420p[${formatTag}]`, // yuva420p for overlay transparency if needed
                `[${formatTag}]setpts=PTS-STARTPTS+${segmentStartTimeInChunk.toFixed(6)}/TB[${finalSegmentVideoTag}]`
      );

            const currentChunkOverlayOutput = `covl${chunkIndex}_${batchSegmentIndex}`;
            // Apply yuv420p format only to the very last overlay in the chain for this chunk for compatibility
            // const overlayPixelFormat = (batchSegmentIndex === batch.length -1) ? ":format=yuv420p" : ""; // REMOVE THIS LINE

            chunkFilterComplexParts.push(
                `[${lastChunkOverlayOutput}][${finalSegmentVideoTag}]overlay=shortest=0:x=0:y=0[${currentChunkOverlayOutput}]`
            );
            lastChunkOverlayOutput = currentChunkOverlayOutput;
        });

        const ffmpegChunkArgs = [
            ...chunkImageInputs,
            '-filter_complex', chunkFilterComplexParts.join(';'),
            '-map', `[${lastChunkOverlayOutput}]`,
            '-map', `${audioInputIndex}:a`,
            '-c:v', 'libx264',
            '-preset', 'fast', 
            '-crf', '23',
            '-pix_fmt', 'yuv420p', // This global option correctly sets the chunk output format
            '-c:a', 'aac',
            '-b:a', '128k',
            '-r', String(fps),
            '-y',
            chunkOutputPath // Absolute path for output
        ];
        
        ffmpegChunkCommands.push({ args: ffmpegChunkArgs, outputChunkPath: chunkOutputPath, chunkIndex });
        chunkOutputFilePaths.push(chunkOutputPath); // Store the absolute path
    }

    // --- Prepare Concatenation ---
    const finalOutputFileName = 'final_output.mp4';
    const finalOutputVideoPath = path.join(tempDir, finalOutputFileName); // Absolute path
    const concatListFileName = 'concat_list.txt';
    const concatListPath = path.join(tempDir, concatListFileName); // Absolute path

    // Generate content for concat_list.txt
    let concatFileContent = "";
    chunkOutputFilePaths.forEach(chunkPath => {
        const relativeChunkPath = path.basename(chunkPath);
        concatFileContent += `file '${relativeChunkPath}'\n`;
    });

    // *** Write the concat_list.txt file directly using Node.js fs ***
    console.log(`Writing concat list to: ${concatListPath}`);
    fs.writeFileSync(concatListPath, concatFileContent.trim()); // Write the file
    console.log(`Content:\\n${concatFileContent.trim()}`);
    // *** End writing concat_list.txt ***

    // Generate args for the final concatenation command
    const ffmpegConcatArgs = [
      '-f', 'concat',
      '-safe', '0',
      '-i', concatListPath,
      '-filter_complex', `[0:v]ass=filename='${subtitlesPath}'[final_v]`,
      '-map', '[final_v]',
      '-map', '0:a',
      '-c:v', 'libx264',
      '-preset', 'fast',
      '-crf', '23',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-b:a', '192k',
      '-r', String(fps),
      '-movflags', '+faststart',
      '-y', finalOutputVideoPath
    ];

    const ffmpegConcatenationCommand = { 
        args: ffmpegConcatArgs 
    };
    
    // --- 8. Generate sequence of command strings for execution ---
    let singleCommandString = "";
    if (ffmpegChunkCommands && ffmpegChunkCommands.length > 0) {
        ffmpegChunkCommands.forEach(chunkCmd => {
            const cmdStr = formatCommandForShell('ffmpeg', chunkCmd.args);
            if (singleCommandString.length > 0) {
                singleCommandString += " && ";
            }
            singleCommandString += cmdStr;
        });
    }

    // Add the final concatenation command
    if (ffmpegConcatenationCommand && ffmpegConcatenationCommand.args) {
        const concatCmdStr = formatCommandForShell('ffmpeg', ffmpegConcatenationCommand.args);
        if (singleCommandString.length > 0) {
            singleCommandString += " && ";
        }
        singleCommandString += concatCmdStr;
    }

    // --- Construct final output for n8n ---
    newItem.json.ffmpegProcessingDetails = {
        isChunkingEnabled: USE_CHUNKING,
        CHUNK_SIZE: CHUNK_SIZE,
        tempDir: tempDir, // For cleanup or access by other nodes
        audioFilePath: audioFilePath, // Absolute path
        imageLocalPaths: downloadedImagePaths, // Array of absolute paths
        subtitlesFilePath: subtitlesExist ? subtitlesPath : null, // Absolute path or null
        subtitlesExist: subtitlesExist,
        audioDuration: audioDuration,
        timedSegments: timedSegments, // For inspection
        collectedSegmentParameters: collectedSegmentParameters, // For inspection
        chunkCommands: ffmpegChunkCommands, // Array of { args, outputChunkPath, chunkIndex }
        concatenationCommand: ffmpegConcatenationCommand, // { args }
        finalOutputVideoPath: finalOutputVideoPath, // Absolute path
        commandsInSequence: singleCommandString, // Ensure this line is present
        outputWidth: outputWidth,
        outputHeight: outputHeight,
        fps: fps
    };
    
    // Cleanup of the old single command output if it exists
    delete newItem.json.ffmpegCommand;
    delete newItem.json.outputVideoPath; // Replaced by finalOutputVideoPath in details

  } catch (error) {
    console.error("Error in n8n Code node (chunking workflow):", error);
    newItem.error = `FFmpeg Command Generation Failed: ${error.message}`;
    newItem.errorStack = error.stack;
    // Ensure tempDir is passed even on error for potential manual cleanup
    if (tempDir) newItem.json.tempDirOnError = tempDir;
  }

  return newItem;
}

// Call the main async function and return its promise
return main();
