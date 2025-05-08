const express = require('express');
const crypto = require('crypto');
const fs = require('fs/promises');
const path = require('path');
const { spawn } = require('child_process');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const morgan = require('morgan'); // For request logging
const { Readable } = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;
const TEMP_DIR_BASE = path.join(__dirname, 'temp');

// --- Configuration ---
const USE_CHUNKING = true; // Set to false to use the original single-command method
const CHUNK_SIZE = 5;      // Number of segments per chunk (if USE_CHUNKING is true)
// --- End Configuration ---

console.log("Setting up express.json middleware...");
app.use(express.json({ limit: '50mb' }));
console.log("express.json middleware configured.");

app.use((req, res, next) => {
    console.log(`Incoming request: ${req.method} ${req.url}`);
    next();
});
console.log("Request logger middleware configured.");

// Ensure base temp directory exists
fs.mkdir(TEMP_DIR_BASE, { recursive: true }).catch(console.error);

app.post('/generate_video', async (req, res) => {
    console.log("Received request for /generate_video");
    const requestId = crypto.randomUUID();
    const tempDir = path.join(TEMP_DIR_BASE, requestId);
    let ffmpegProcess = null;
    const chunkOutputFilePaths = [];

    const cleanupAndExit = async (exitCode = 1) => {
        console.log(`
Initiating cleanup for request ${requestId}...`);
        if (ffmpegProcess && !ffmpegProcess.killed) {
            console.log('Attempting to kill ffmpeg process...');
            const killed = ffmpegProcess.kill('SIGKILL');
            console.log(`ffmpeg process killed: ${killed}`);
            await new Promise(resolve => setTimeout(resolve, 500));
        } else {
            console.log('No active ffmpeg process to kill or already killed.');
        }
        try {
            console.log(`Removing temporary directory: ${tempDir}`);
            await fs.rm(tempDir, { recursive: true, force: true });
            console.log(`Temporary directory removed: ${tempDir}`);
        } catch (cleanupError) {
            console.error(`Error during cleanup for ${requestId}:`, cleanupError);
        }
        if (!res.headersSent) {
             res.status(500).json({ error: 'Process interrupted during cleanup.' });
        }
        process.exit(exitCode);
    };

    process.once('SIGINT', () => cleanupAndExit(2));
    process.once('SIGTERM', () => cleanupAndExit(15));

    process.once('uncaughtException', async (err) => {
        console.error('Uncaught Exception:', err);
        await cleanupAndExit(1);
    });
    process.once('unhandledRejection', async (reason, promise) => {
        console.error('Unhandled Rejection at:', promise, 'reason:', reason);
        await cleanupAndExit(1);
    });

    try {
        try {
            await fs.mkdir(tempDir, { recursive: true });
            console.log(`Created temp directory: ${tempDir}`);
        } catch (mkdirError) {
            console.error(`Error creating temp directory ${tempDir}:`, mkdirError);
            return res.status(500).json({ error: 'Failed to create temporary directory.', details: mkdirError.message });
        }

        console.log("Confirmed req.body exists.");
        // console.log("req.body content:", JSON.stringify(req.body, null, 2)); // Keep commented out

        // Access segments array
        const segments = req.body.segments;
        console.log("Accessed req.body.segments");

        // --- Input Validation ---
        // Validate segments array presence
        if (!Array.isArray(segments) || segments.length === 0) {
            console.error("Validation failed: segments is not a non-empty array");
            return res.status(400).json({ error: 'Invalid input: "segments" must be a non-empty array.' });
        }
        console.log("Validated segments array presence");
        
        // Validate root-level audioInBase64
        const audioInBase64 = req.body.audioInBase64;
        if (!audioInBase64) {
            console.error("Validation failed: root-level audioInBase64 missing");
            return res.status(400).json({ error: 'Invalid input: "audioInBase64" is missing at the root level.' });
        }
        console.log("Validated root-level audioInBase64 presence");
        
        // Validate root-level transcript
        const transcript = req.body.transcript;
        if (!Array.isArray(transcript) || transcript.length === 0) {
            console.error("Validation failed: root-level transcript missing or empty");
            return res.status(400).json({ error: 'Invalid input: "transcript" is missing or empty at the root level.' });
        }
        console.log("Validated root-level transcript presence");

        // Access firstSegment for other properties if needed, but not audio/transcript
        // const firstSegment = segments[0]; // Might not be needed immediately now
        // console.log("Accessed firstSegment"); 
        

        console.log(`Processing ${segments.length} segment(s) for request ${requestId}`);

        // --- 1. Decode Audio & Get Duration --- 
        const audioFilePath = path.join(tempDir, 'audio.mp3');
        console.log("Attempting to decode audio from base64...");
        // Use the root-level audioInBase64
        const audioBuffer = Buffer.from(audioInBase64, 'base64');
        console.log("Successfully created audio buffer from base64");
        console.log("Attempting to write audio file...");
        await fs.writeFile(audioFilePath, audioBuffer);
        console.log(`Decoded audio saved to: ${audioFilePath}`);

        let audioDuration;
        try {
            const ffprobeArgs = [
                '-v', 'error', 
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                audioFilePath
            ];
            const { process: ffprobeProc, commandPromise: ffprobePromise } = await runCommand('ffprobe', ffprobeArgs);
            const { stdout } = await ffprobePromise; 
            audioDuration = parseFloat(stdout.trim());
            if (isNaN(audioDuration) || audioDuration <= 0) {
                throw new Error('ffprobe could not determine audio duration or duration is invalid.');
            }
            console.log(`Audio duration: ${audioDuration} seconds`);
        } catch (err) {
            console.error('Error running ffprobe:', err);
            return res.status(500).json({ 
                error: 'Failed to get audio duration.', 
                details: err.message, 
                stderr: err.stderr 
            }); 
        }
 
        // --- 2. Download Images --- 
        const imagePaths = [];
        // ... (image download logic remains the same, uses segments array) ...
        const downloadPromises = segments.map(async (segment, index) => {
            if (!segment.images || !Array.isArray(segment.images) || segment.images.length === 0 || !segment.images[0].url) {
                throw new Error(`Segment ${index} is missing a valid image URL in images[0].url`);
            }
            const imageUrl = segment.images[0].url;
            const extension = path.extname(new URL(imageUrl).pathname) || '.jpg';
            const imagePath = path.join(tempDir, `image_${index}${extension}`);
            
            try {
                console.log(`Downloading image ${index}: ${imageUrl}`);
                const response = await axios.get(imageUrl, { 
                    responseType: 'arraybuffer',
                    timeout: 30000 // 30 seconds timeout
                 });
                await fs.writeFile(imagePath, response.data);
                console.log(`Saved image ${index} to: ${imagePath}`);
                imagePaths[index] = imagePath;
            } catch (downloadError) {
                console.error(`Failed to download image ${index} from ${imageUrl}:`, downloadError);
                throw new Error(`Failed to download image for segment ${index}: ${downloadError.message}`);
            }
        });

        try {
            await Promise.all(downloadPromises);
            console.log("All images downloaded successfully.");
        } catch (error) {
             console.error("Error during image download:", error);
             return res.status(400).json({ error: 'Failed to download one or more images.', details: error.message });
        }


        // --- 3. Calculate Timeline --- 
        const timedSegments = [];
        const defaultFixedDuration = 5.0;
        const minVisualDuration = 0.1;

        // --- Pass 1: Pre-calculate dynamic timings ---
        console.log("--- Pre-calculating Dynamic Timings ---");
        const dynamicTimings = segments.map((segment, i) => {
            const durationMode = (segment.durationMode || req.body.durationMode || 'fixed');
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
                    console.warn(`Segment ${i} (dynamic-precalc): Could not find text "${segment.textOnScreen}".`);
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
            const durationMode = (segment.durationMode || req.body.durationMode || 'fixed');
            
            let segmentTiming = {
                display_start_time: 0,
                display_end_time: 0,
                effective_visual_duration: 0,
                speech_duration: 0,
                index: i
            };

            if (precalc.dynamicSuccess) {
                segmentTiming.display_start_time = Math.max(videoTime, precalc.startTime);
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
            
            // Ensure end time respects minimum duration if dynamic end time was too close to start
             segmentTiming.display_end_time = segmentTiming.display_start_time + segmentTiming.effective_visual_duration;

            // --- Adjust LAST segment --- 
            if (i === segments.length - 1) {
                 // Clamp the end time to the audio duration
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
        
        // Overlap correction loop removed as the new sequential placement logic should prevent major overlaps
        // requiring drastic start time shifts.

        // --- 4. Generate Subtitles --- 
        let subtitlesPath = null;
        if (transcript && transcript.length > 0) {
            subtitlesPath = path.join(tempDir, 'subtitles.ass');
            try {
                 // Use the root-level transcript
                 const assContent = generateAssSubtitles(transcript);
                 await fs.writeFile(subtitlesPath, assContent);
                 console.log(`Generated ASS subtitles: ${subtitlesPath}`);
            } catch (subError) {
                 console.error("Error generating subtitles:", subError);
                 console.warn("Proceeding without subtitles due to generation error.");
            }
        } else {
            console.log("No transcript data found, skipping subtitle generation.");
        }

        const outputVideoPath = path.join(tempDir, 'output.mp4');
        const ffmpegCommandOptions = {
            imagePaths,
            audioFilePath,
            subtitlesPath,
            outputPath: outputVideoPath,
            timedSegments,
            audioDuration,
            outputWidth: req.body.outputWidth || 1920,
            outputHeight: req.body.outputHeight || 1080
        };
        
        const { command, args, collectedSegmentParameters } = constructFfmpegCommand(ffmpegCommandOptions);

        if (USE_CHUNKING) {
            // --- Chunking Workflow ---
            console.log("Using chunking workflow.");

            if (!collectedSegmentParameters || collectedSegmentParameters.length === 0) {
                throw new Error("No segment parameters collected for chunk processing.");
            }

            // --- 1. Generate A/V Chunks ---
            for (let i = 0; i < collectedSegmentParameters.length; i += CHUNK_SIZE) {
                const batch = collectedSegmentParameters.slice(i, i + CHUNK_SIZE);
                if (batch.length === 0) continue;

                const chunkIndex = i / CHUNK_SIZE;
                const chunkOutputFileName = `video_chunk_${chunkIndex}.mp4`;
                const chunkOutputPath = path.join(tempDir, chunkOutputFileName);
                console.log(`Processing chunk ${chunkIndex}... Output: ${chunkOutputPath}`);

                const chunkImageInputs = [];
                const chunkFilterComplexParts = [];
                
                if (!timedSegments || timedSegments.length === 0) {
                    throw new Error("timedSegments is not available for chunk processing.");
                }
                
                const firstSegmentOriginalIndex = batch[0].originalIndex;
                const lastSegmentOriginalIndex = batch[batch.length - 1].originalIndex;

                if (timedSegments[firstSegmentOriginalIndex] === undefined || timedSegments[lastSegmentOriginalIndex] === undefined) {
                    throw new Error(`Invalid segment index for chunk ${chunkIndex}. Cannot determine chunk start/end times.`);
                }

                const chunkGlobalStartTime = timedSegments[firstSegmentOriginalIndex].display_start_time;
                // Use the end time of the *last* segment in the batch for audio end time
                const chunkGlobalEndTime = timedSegments[lastSegmentOriginalIndex].display_end_time; 
                // Duration for the black background should cover the visual elements of the chunk
                const chunkVideoDuration = Math.max(0.1, timedSegments[lastSegmentOriginalIndex].display_end_time - chunkGlobalStartTime); 
                
                if (isNaN(chunkVideoDuration) || isNaN(chunkGlobalStartTime) || isNaN(chunkGlobalEndTime)) {
                     throw new Error(`Calculated timing is NaN for chunk ${chunkIndex}. Start: ${chunkGlobalStartTime}, End: ${chunkGlobalEndTime}, VidDur: ${chunkVideoDuration}`);
                }

                // Add image inputs
                batch.forEach((segmentParams) => {
                    chunkImageInputs.push('-i', segmentParams.imagePath);
                });
                // Add audio input for the specific time range
                const audioInputIndex = chunkImageInputs.length / 2; // Inputs are added in pairs ('-i', path)
                chunkImageInputs.push('-ss', chunkGlobalStartTime.toFixed(6)); // Use precise start time
                chunkImageInputs.push('-to', chunkGlobalEndTime.toFixed(6));   // Use precise end time
                chunkImageInputs.push('-i', audioFilePath);

                // Filter complex setup
                chunkFilterComplexParts.push(`color=black:s=${batch[0].outputWidth}x${batch[0].outputHeight}:d=${chunkVideoDuration.toFixed(6)}[base_chunk_${chunkIndex}]`);
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
                        `[${zoompanTag}]format=pix_fmts=yuva420p[${formatTag}]`,
                        `[${formatTag}]setpts=PTS-STARTPTS+${segmentStartTimeInChunk.toFixed(6)}/TB[${finalSegmentVideoTag}]`
                    );

                    const currentChunkOverlayOutput = `covl${chunkIndex}_${batchSegmentIndex}`;
                    chunkFilterComplexParts.push(
                        `[${lastChunkOverlayOutput}][${finalSegmentVideoTag}]overlay=shortest=0:x=0:y=0[${currentChunkOverlayOutput}]`
                    );
                    lastChunkOverlayOutput = currentChunkOverlayOutput;
                });

                const ffmpegChunkArgs = [
                    ...chunkImageInputs,
                    '-filter_complex', chunkFilterComplexParts.join(';'),
                    '-map', `[${lastChunkOverlayOutput}]`, // Map final chunk video
                    '-map', `${audioInputIndex}:a`,       // Map the corresponding audio segment
                    '-c:v', 'libx264',
                    '-preset', 'fast', 
                    '-crf', '23',
                    '-pix_fmt', 'yuv420p',
                    '-c:a', 'aac',      // Encode audio for chunk
                    '-b:a', '128k',     // Standard bitrate
                    '-shortest',        // Ensure output duration is trimmed to shortest input (audio segment)
                    '-y',
                    chunkOutputPath
                ];
                
                console.log(`Executing chunk ${chunkIndex} command: ffmpeg ${ffmpegChunkArgs.join(' ')}`);
                const { commandPromise: chunkCommandPromise } = await runCommand('ffmpeg', ffmpegChunkArgs, { cwd: tempDir });
                await chunkCommandPromise; 
                console.log(`Chunk ${chunkIndex} generated successfully.`);
                chunkOutputFilePaths.push(chunkOutputFileName); 
            }

            // --- 2. Concatenate A/V Chunks ---
            console.log("All A/V chunks generated. Starting concatenation...");

            if (chunkOutputFilePaths.length === 0) {
                throw new Error("No video chunks were generated to concatenate.");
            }

            const concatListPath = path.join(tempDir, 'concat_list.txt');
            const concatFileContent = chunkOutputFilePaths.map(p => `file '${p.replace(/\\/g, '/')}'`).join('\n');
            await fs.writeFile(concatListPath, concatFileContent);
            console.log(`Generated concat list: ${concatListPath}`);

            const ffmpegConcatArgs = [
                '-f', 'concat', '-safe', '0', '-i', concatListPath, // Use absolute path again
                '-i', audioFilePath // Keep absolute path for audio input
            ];

            const concatFilterComplexParts = [];
            let videoMap = '[0:v]';
            let audioMap = '[0:a]';
            let videoCodecOpts = ['-c:v', 'copy']; // Default video codec
            const audioCodecOpts = ['-c:a', 'aac', '-b:a', '128k']; // Always re-encode audio for compatibility

            if (subtitlesPath) {
                console.log("Applying subtitles during concatenation.");
                const escapedSubsPath = subtitlesPath.replace(/\\/g, '/').replace(/:/g, '\\:');
                concatFilterComplexParts.push(`[0:v]ass='${escapedSubsPath}'[final_v]`);
                concatFilterComplexParts.push(`[0:a]anull[final_a]`); // Pass audio through
                videoMap = '[final_v]';
                audioMap = '[final_a]';
                ffmpegConcatArgs.push('-filter_complex', concatFilterComplexParts.join(';'));
                // Re-encoding video is necessary when applying filters
                videoCodecOpts = [
                    '-c:v', 'libx264', 
                    '-preset', 'fast',
                    '-crf', '23',
                    '-pix_fmt', 'yuv420p' // Ensure final pixel format
                ];
            } else {
                console.log("No subtitles to apply, using video stream copy for concatenation.");
                // No filter_complex needed if just copying video & re-encoding audio
            }
            
            ffmpegConcatArgs.push(
                '-map', videoMap,
                '-map', audioMap,
                ...videoCodecOpts, // Apply determined video codec options
                ...audioCodecOpts, // Always apply audio encoding options
                '-movflags', '+faststart',
                '-y',
                outputVideoPath 
            );

            console.log(`Executing concatenation command: ffmpeg ${ffmpegConcatArgs.join(' ')}`);
            const { process: concatProcess, commandPromise: concatCommandPromise } = await runCommand('ffmpeg', ffmpegConcatArgs, { cwd: tempDir });
            ffmpegProcess = concatProcess; // Assign for potential cleanup
            
            await concatCommandPromise;

        } else {
            // --- Original Single-Command Workflow ---
            console.log("Using single-command workflow.");
            // 'command' and 'args' were already generated by constructFfmpegCommand
            if (!command || !args) {
                 throw new Error("Failed to construct FFmpeg command arguments for single-command workflow.");
            }
            console.log(`Executing single command: ${command} ${args.join(' ')}`);
            const { process, commandPromise } = await runCommand(command, args, { cwd: tempDir });
            ffmpegProcess = process; // Assign for potential cleanup

            const { /* stdout, stderr, code */ } = await commandPromise;
            // Error handling is implicitly done by the promise rejection in runCommand
            console.log("FFmpeg process finished for single command.");
        }

        // --- Send File (Common to both workflows) ---
        console.log(`Video generation complete: ${outputVideoPath}`);
        res.sendFile(outputVideoPath, (err) => {
             if (err) {
                console.error('Error sending file:', err);
                 if (!res.headersSent) {
                     res.status(500).json({ error: 'Failed to send the generated video file.' });
                 }
             } else {
                 console.log('Video file sent successfully.');
             }
        });

    } catch (error) {
        console.error('Error during video generation pipeline:', error);
        // Ensure cleanup runs even on pipeline error before responding
        await cleanupAndExit(1); 
        if (!res.headersSent) {
            res.status(500).json({ error: 'Video generation failed.', details: error.message });
        }
    } finally {
        // Remove signal handlers
        process.removeListener('SIGINT', cleanupAndExit);
        process.removeListener('SIGTERM', cleanupAndExit);
        process.removeListener('uncaughtException', cleanupAndExit);
        process.removeListener('unhandledRejection', cleanupAndExit);
        
        console.log(`Starting final cleanup for request ${requestId}`);
        try {
            // Conditionally delete chunk files and concat list if chunking was used
            if (USE_CHUNKING) {
                for (const chunkRelPath of chunkOutputFilePaths) {
                    try {
                        await fs.unlink(path.join(tempDir, chunkRelPath));
                        console.log(`Deleted chunk: ${chunkRelPath}`);
                    } catch (unlinkErr) {
                        // Log warning but continue cleanup
                        console.warn(`Could not delete chunk ${chunkRelPath}: ${unlinkErr.message}`);
                    }
                }
                try {
                    await fs.unlink(path.join(tempDir, 'concat_list.txt'));
                    console.log("Deleted concat_list.txt");
                } catch (unlinkErr) {
                     console.warn(`Could not delete concat_list.txt: ${unlinkErr.message}`);
                }
            }
            
            // Always attempt to remove the main temp directory
            await fs.access(tempDir); 
            await fs.rm(tempDir, { recursive: true, force: true });
            console.log(`Successfully cleaned up temp directory: ${tempDir}`);
        } catch (cleanupError) {
            if (cleanupError.code !== 'ENOENT') { // Ignore if dir already gone
                console.error(`Error during final cleanup for ${requestId}:`, cleanupError);
            }
        }
        console.log(`Request ${requestId} processing finished.`);
    }
});

console.log("About to call app.listen...");
app.listen(PORT, () => {
    console.log(`Server listening callback executed. Port: ${PORT}`);
});
console.log("app.listen called.");

function normalizeText(text) {
    if (!text) return '';
    return text.toLowerCase().replace(/[.,!?;:'"-]/g, '').trim();
}

function findWordSequence(transcript, text) {
    if (!transcript || transcript.length === 0 || !text) {
        return null; 
    }

    const normalizedText = normalizeText(text);
    const targetWords = normalizedText.split(/\s+/).filter(w => w.length > 0);

    if (targetWords.length === 0) {
        return null;
    }

    const transcriptWords = transcript.map(item => ({
        ...item,
        normalizedWord: normalizeText(item.word)
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
                endIndex: i + targetWords.length - 1
            };
        }
    }

    console.warn(`Could not find sequence "${text}" (normalized: "${normalizedText}") in transcript.`);
    return null;
}

function formatAssTime(seconds) {
    const hh = Math.floor(seconds / 3600);
    const mm = Math.floor((seconds % 3600) / 60);
    const ss = Math.floor(seconds % 60);
    const cs = Math.round((seconds - Math.floor(seconds)) * 100);
    return `${hh}:${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')}.${String(cs).padStart(2, '0')}`;
}

// Generate ASS subtitle content from transcript (Word-by-Word)
function generateAssSubtitles(transcript, style = {}) {
    const defaultStyle = {
        Name: 'Default',
        Fontname: 'Arial',
        Fontsize: '28',
        PrimaryColour: '&H00FFFFFF', 
        SecondaryColour: '&H000000FF',
        OutlineColour: '&H00000000',
        BackColour: '&H80000000',
        Bold: '0',
        Italic: '0',
        Underline: '0',
        StrikeOut: '0',
        ScaleX: '100',
        ScaleY: '100',
        Spacing: '0',
        Angle: '0',
        BorderStyle: '1', 
        Outline: '1.5',
        Shadow: '1',
        Alignment: '2', 
        MarginL: '10',
        MarginR: '10',
        MarginV: '20',
        Encoding: '1'
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

    // Generate one Dialogue event per word
    for (let i = 0; i < transcript.length; i++) {
        const wordInfo = transcript[i];
        const start = wordInfo.start;
        const end = wordInfo.end;
        const word = wordInfo.word;
        const duration = end - start;

        // Skip words with zero or negative duration, or handle them differently if needed
        if (duration <= 0) {
            console.warn(`Skipping word "${word}" at index ${i} due to non-positive duration (${duration.toFixed(3)}s)`);
            continue; 
        }

        // Use the original word. No complex escaping needed for basic text here.
        // Remove the trailing \N as each word is its own event line
        // Add a standard JavaScript newline \n after each complete Dialogue line for file formatting.
        assContent += `Dialogue: 0,${formatAssTime(start)},${formatAssTime(end)},${mergedStyle.Name},,0,0,0,,${word}\n`;
        // depending on desired layout. Remove \N if you want words to flow on one line.
    }

    return assContent;
}

function constructFfmpegCommand(options) {
    const {
        imagePaths,
        audioFilePath,
        subtitlesPath,
        outputPath,
        timedSegments,
        audioDuration,
        outputWidth = 1920,
        outputHeight = 1080
    } = options;

    const ffmpegArgs = [];
    const inputs = [];
    const filterComplexParts = [];
    const fps = 60;

    const collectedSegmentParameters = [];

    // --- Inputs ---
    imagePaths.forEach((imgPath) => {
        inputs.push('-i', imgPath);
    });
    inputs.push('-i', audioFilePath);
    ffmpegArgs.push(...inputs);

    // --- Filter Complex ---
    const baseCanvasTag = 'base';
    filterComplexParts.push(`color=black:s=${outputWidth}x${outputHeight}:d=${audioDuration}[${baseCanvasTag}]`);

    let lastOverlayOutput = baseCanvasTag;

    timedSegments.forEach((segment, index) => {
        const imgInputIndex = index;
        const start = segment.display_start_time;
        const speechDuration = segment.speech_duration;
        const zoompanTag = `zoompan${index}`;
        const formatTag = `format${index}`;
        const finalSegmentVideoTag = `v${index}`;

        const zoompanDurationSeconds = Math.max(0.1, speechDuration);
        const zoompanDurationFrames = Math.ceil(zoompanDurationSeconds * fps);

        let zoompanX, zoompanY;
        if (index % 2 === 0) {
            zoompanX = '0*(zoom-1)';
            zoompanY = 'ih-ih/zoom';
        } else {
            zoompanX = 'iw-iw/zoom';
            zoompanY = '0';
        }
        const zoomExpression = 'if(eq(on,1),1,min(zoom+0.0010,1.5))';
        const zoompanFilter = `zoompan=z='${zoomExpression}':x='${zoompanX}':y='${zoompanY}':d=${zoompanDurationFrames}:s=${outputWidth}x${outputHeight}:fps=${fps}`;
        
        filterComplexParts.push(
            `[${imgInputIndex}:v]${zoompanFilter}[${zoompanTag}]`,
            `[${zoompanTag}]format=pix_fmts=yuva420p[${formatTag}]`
        );
        filterComplexParts.push(
            `[${formatTag}]setpts=PTS-STARTPTS+${start}/TB[${finalSegmentVideoTag}]`
        );

        const currentOverlayOutput = `ovl${index}`;
        const overlayFormat = (index === timedSegments.length - 1) ? ':format=yuv420' : '';
        filterComplexParts.push(
            `[${lastOverlayOutput}][${finalSegmentVideoTag}]overlay=shortest=0:x=0:y=0${overlayFormat}[${currentOverlayOutput}]`
        );
        lastOverlayOutput = currentOverlayOutput;

        collectedSegmentParameters.push({
            originalIndex: index,
            imagePath: imagePaths[index],
            zoomExpression: zoomExpression,
            zoompanX: zoompanX,
            zoompanY: zoompanY,
            zoompanDurationFrames: zoompanDurationFrames,
            outputWidth: outputWidth,
            outputHeight: outputHeight,
            fps: fps,
            displayStartTimeGlobal: segment.display_start_time,
            effectiveVisualDurationGlobal: segment.effective_visual_duration,
            speechDurationGlobal: segment.speech_duration,
        });
    });

    // --- Subtitles ---
    let finalVideoOutputTag = lastOverlayOutput;
    if (subtitlesPath) {
        const subtitlesOutputTag = 'subtitled';
        const escapedSubsPath = subtitlesPath.replace(/\\/g, '/').replace(/:/g, '\\:');
        filterComplexParts.push(
             `[${lastOverlayOutput}]ass='${escapedSubsPath}'[${subtitlesOutputTag}]`
        );
         finalVideoOutputTag = subtitlesOutputTag;
    }

    ffmpegArgs.push('-filter_complex', filterComplexParts.join(';'));

    // --- Output Mapping and Options ---
    ffmpegArgs.push(
        '-map', `[${finalVideoOutputTag}]`,
        '-map', `${imagePaths.length}:a`,
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '23',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart',
        '-y',
        outputPath
    );

    return { command: 'ffmpeg', args: ffmpegArgs, collectedSegmentParameters };
}

async function runCommand(command, args, options = {}) {
    return new Promise((resolve, reject) => {
        console.log(`Executing: ${command} ${args.join(' ')}`);
        const process = spawn(command, args, { ...options, stdio: ['pipe', 'pipe', 'pipe'] });
        let stdout = '';
        let stderr = '';

        const commandPromise = new Promise((resolveCmd, rejectCmd) => {
            process.stdout.on('data', (data) => {
                stdout += data.toString();
            });

            process.stderr.on('data', (data) => {
                stderr += data.toString();
                console.error(`stderr: ${data}`);
            });

            process.on('close', (code) => {
                console.log(`${command} process exited with code ${code}`);
                if (code === 0) {
                    resolveCmd({ stdout, stderr, code });
                } else {
                    const error = new Error(`Command failed with code ${code}: ${command} ${args.join(' ')}`);
                    error.stdout = stdout;
                    error.stderr = stderr;
                    error.code = code;
                    rejectCmd(error);
                }
            });

            process.on('error', (err) => {
                console.error(`Failed to start subprocess ${command}:`, err);
                 err.stderr = stderr;
                 err.stdout = stdout; 
                rejectCmd(err); 
            });
        });

        resolve({ process, commandPromise }); 
    });
} 