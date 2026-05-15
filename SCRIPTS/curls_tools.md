rvc:
curl --location 'https://superexquisite-johan-curmudgeonly.ngrok-free.dev/aitools/voice-remix/v1/convert' \
--form 'audio_file=@"/C:/Users/Lenovo/Downloads/ttsmaker-file-2026-2-5-21-40-26_vN6RGHqD.mp3"' \
--form 'consent="true"' \
--form 'expression_intensity="100"' \
--form 'vibrato_depth="30"' \
--form 'pitch_correction="false"' \
--form 'formant_preservation="false"' \
--form 'noise_reduction="10"' \
--form 'user_id="36242f8f-5753-4e21-9e12-dcb054a73a25"' \
--form 'singer="Mohammad rafi"'

voice clone
curl --location 'https://test-apigateway.erosuniverse.com/aitools/voice-clone/v1/generate' \
--header 'Content-Type: multipart/form-data' \
--form 'user_id="5a1032b5-d53a-4e86-a33c-35a2a938ed9b"' \
--form 'reference_audio=@"/D:/Load-Test-Scripts/INPUTS/AI-TOOLS/Voice Remix/malayalam.mp3"' \
--form 'action="upload"'

curl --location 'https://test-apigateway.erosuniverse.com/aitools/voice-clone/v1/generate' \
--header 'Content-Type: multipart/form-data' \
--form 'user_id="5f5a225d-5f4f-4a75-986a-6399e1bcf7f5"' \
--form 'input_text="Her parents were nearby, keeping an eye on her as she ran around with excitement."' \
--form 'reference_audio_url="https://mediad.erosuniverse.com/prodimmersobuk01/tools/voice-cloning/input/0294c2a7-8c20-4577-acdb-45be6c0f4b55_reference_reference_audio.wav?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcs-presigned-sa@test-483907.iam.gserviceaccount.com/20260211/auto/storage/goog4_request&X-Goog-Date=20260211T100106Z&X-Goog-Expires=86400&X-Goog-SignedHeaders=host&response-content-disposition=inline&X-Goog-Signature=6ae109b5f91c76f3124dd81bbe1b95602c17a72394b4d2bd91529695574851afde6ce977107ceb1fc9590b5fddabf16404a73cb00cfad7fc7bf49042a4d915a5740e55fe90bf90f00634c5d8cde228dbd6a39fa165dea12d4bd52f20c413787c250bff06487493ad715c5549ec45cb4e0a3909e6f1e68744e89bcee9d9c94b15ccc856ae7d238bf537ab3a621d38f567082fe2b0471fc6d40e282bcdfbabd789b8aaf8c2513f7233eb5ed3947381d090b9d7873b47c0c9bb034cbb284394367d4d1af395893f66ff75287cf9f9eb762abda82c2d0bd098d511bae9bd2e68c2dc8d70bccbfec9087437180601ecc7e2d2c178680ce1eb406834a2b72fd10ebcc6"' \
--form 'action="generate"' \
--form 'generation_id="73e1f60d-7f59-4a05-b08a-4a37d5524c9d"'

colorise 
curl --location 'https://apigateway.erosuniverse.com/aitools/colorize/v1/generate' \
--form 'image=@"/D:/Input-Datas/Latest_Feb_6/Face-Swap/Images/Input2.jpg"' \
--form 'user_id="{{generation_id}}"' \
--form 'mode="Black&White"' \
--form 'prompt="make this image more vibrant"' \
--form 'preset="Warm"'

ifs:
template
curl --location 'https://apigateway.erosuniverse.com/aitools/face-swap/v1/upload-template' \
--form 'user_id="5f5a225d-5f4f-4a75-986a-6399e1bcf7f5"' \
--form 'files=@"/C:/Users/Lenovo/Downloads/rajinikanth-7593.jpg"'

target:
curl --location 'https://apigateway.erosuniverse.com/aitools/face-swap/v1/upload-target-files' \
--form 'generation_id="2ebb3259-8bd4-480d-b500-0d57712edf05"' \
--form 'user_id="5f5a225d-5f4f-4a75-986a-6399e1bcf7f5"' \
--form 'files=@"/C:/Users/Lenovo/Downloads/rajinikanth-7593.jpg"'

face swap:
curl --location 'https://apigateway.erosuniverse.com/aitools/face-swap/v1/generate' \
--header 'Content-Type: application/json' \
--data-raw '{
    "generation_id": "2ebb3259-8bd4-480d-b500-0d57712edf05",
    "target_file_urls": 
        [
        "https://storage.googleapis.com/prodimmersobuk01/tools/face-swap/target-faces/2d41d44a-5c63-4c32-a28e-a950d2ccd82f/face_0.jpg?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcs-presigned-sa@test-483907.iam.gserviceaccount.com/20260211/auto/storage/goog4_request&X-Goog-Date=20260211T184205Z&X-Goog-Expires=86400&X-Goog-SignedHeaders=host&response-content-disposition=inline&X-Goog-Signature=9459cba792ad13c54aac162a8d16a640532549b52ccab905199824f6626c49e41aed13c69ce10622233a5306edb0ee84cd9f743aa7ea242cd80db45169e36391290589b687704b05c10caab49261bacd949b430f7faec548447aec3938cfcb7db0d762620b7ebf83adbf80523dcec9b9a46ac344219f5a1f86bc013312070285bd4f69564939bbd43fe45cb88cae2db58283a4fd885ee07281db747a606778038f5edd23530cc4e227d5a7317fdee768c2864b9f8262f018af8fb3ed015e741fc5cadd9c9f7acd603a60dc5702f8c87041dcd1644891e8280b1f0d7666daa62ad7b489e54f5764c2ead5b148528b44c969709db296638355d2fc6b6e9aac2e00"
        ]
}'


vfs:
template video
curl --location 'https://apigateway.erosuniverse.com/aitools/video-face-swap/v1/uploadvideo/' \
--header 'Content-Type: multipart/form-data' \
--form 'user_id="5f5a225d-5f4f-4a75-986a-6399e1bcf7f5"' \
--form 'video=@"/C:/Users/Lenovo/OneDrive - Eros Invest/Testing Files/nsfw/bad_words2.mp4"'

target image:
curl --location 'https://apigateway.erosuniverse.com/aitools/video-face-swap/v1/uploadtargetface/511edc64-f72e-4d5d-b6f9-3d3be4e15e4a' \
--form 'file=@"/C:/Users/HP/Downloads/Media (5).jpg"'

bind face :
curl --location 'https://apigateway.erosuniverse.com/aitools/video-face-swap/v1/uploadnewfaces/511edc64-f72e-4d5d-b6f9-3d3be4e15e4a/0' \
--header 'Content-Type: multipart/form-data' \
--form 'image_url="https://sosnm1.shakticloud.ai:9024/prodimmersobuk02/VFS/target-faces/b4454860-3e35-41e6-b4f3-79136ebd7c50/target_face_b4454860-3e35-41e6-b4f3-79136ebd7c50_face_0_1769696020.jpg?response-content-disposition=inline&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=immersouser%2F20260129%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260129T141340Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=5a9a119934524f8cd96844d8117edc50b30cc5b0272c9e9ef28f87533ec700ff"'

performa swap 
curl --location 'https://apigateway.erosuniverse.com/aitools/video-face-swap/v1/faceswap/511edc64-f72e-4d5d-b6f9-3d3be4e15e4a' \
--header 'Content-Type: application/json' \
--data '{
    "group_ids": [0]
}'
