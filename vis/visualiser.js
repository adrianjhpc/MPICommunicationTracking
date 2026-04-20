// Global State
let parsedData = null;
let nodeMap = new Map();

// Playback State
let isPlaying = false;
let currentTime = 0;
let maxTime = 0;
let animationFrameId = null;
const TIME_WINDOW = 0.05; 

// Three.js Core Variables
let scene, camera, renderer, controls;
let linesGroup;

document.addEventListener("DOMContentLoaded", () => {
    initThreeJS();
    document.getElementById("profileLoader").addEventListener("change", handleFileUpload);
    document.getElementById("timeSlider").addEventListener("input", handleManualSeek);
    document.getElementById("btn-play").addEventListener("click", togglePlayback);
});

function initThreeJS() {
    const container = document.getElementById('visCanvas');
    
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0d1117);

    // Camera setup
    camera = new THREE.PerspectiveCamera(60, container.clientWidth / container.clientHeight, 1, 1000);
    camera.position.set(0, 50, 150);

    // Renderer setup
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(renderer.domElement);

    // Controls setup (allows dragging to rotate, scrolling to zoom)
    controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;

    // Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.4);
    scene.add(ambientLight);
    const pointLight = new THREE.PointLight(0x58a6ff, 1);
    pointLight.position.set(50, 100, 50);
    scene.add(pointLight);

    // Group to hold active communication lines
    linesGroup = new THREE.Group();
    scene.add(linesGroup);

    // Start render loop
    const animate = function () {
        requestAnimationFrame(animate);
        controls.update();
        renderer.render(scene, camera);
    };
    animate();

    // Handle window resize
    window.addEventListener('resize', () => {
        camera.aspect = container.clientWidth / container.clientHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(container.clientWidth, container.clientHeight);
    });
}

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            parsedData = JSON.parse(e.target.result);
            initDashboard();
        } catch (error) {
            console.error(error);
        }
    };
    reader.readAsText(file);
}

function initDashboard() {
    pausePlayback();
    nodeMap.clear();

    // Clear existing nodes and lines from the scene
    const objectsToRemove = [];
    scene.traverse(child => {
        if (child.name === "mpiNode" || child.name === "cabinetBox") {
            objectsToRemove.push(child);
        }
    });
    objectsToRemove.forEach(obj => scene.remove(obj));
    clearLines();

    const timeline = parsedData.timeline;
    const topology = parsedData.topology;

    maxTime = timeline.length > 0 ? timeline[timeline.length - 1].time : 0;
    
    document.getElementById("timeSlider").max = maxTime;
    document.getElementById("timeSlider").disabled = false;
    document.getElementById("btn-play").disabled = false;

    buildHardwareTopology(topology);
    seekToTime(0);
}

function buildHardwareTopology(nodesData) {
    const nodeGeometry = new THREE.BoxGeometry(8, 8, 8);
    const nodeMaterial = new THREE.MeshPhongMaterial({ color: 0x8b949e, emissive: 0x222222 });

    nodesData.forEach(d => {
        const mesh = new THREE.Mesh(nodeGeometry, nodeMaterial);
        mesh.position.set(d.x, d.y, d.z);
        mesh.name = "mpiNode";
        
        scene.add(mesh);
        
        // Save to map for fast line drawing later
        nodeMap.set(d.rank, { x: d.x, y: d.y, z: d.z, mesh: mesh });
    });
}

function handleManualSeek(event) {
    pausePlayback();
    seekToTime(parseFloat(event.target.value));
}

function seekToTime(time) {
    currentTime = time;
    document.getElementById("timeSlider").value = currentTime;
    document.getElementById("currentTimeLabel").textContent = currentTime.toFixed(3);
    renderActiveCommunications();
}

function renderActiveCommunications() {
    clearLines();

    const activeEvents = parsedData.timeline.filter(d => 
        d.time <= currentTime && d.time >= (currentTime - TIME_WINDOW)
        && d.sender !== d.receiver 
    );

    const material = new THREE.LineBasicMaterial({ 
        color: 0xff7b72, 
        transparent: true, 
        opacity: 0.8,
        linewidth: 2 // Note: WebGL standard limits lines to 1px wide on many systems
    });

    activeEvents.forEach(event => {
        const sender = nodeMap.get(event.sender);
        const receiver = nodeMap.get(event.receiver);

        if (sender && receiver) {
            const points = [];
            points.push(new THREE.Vector3(sender.x, sender.y, sender.z));
            points.push(new THREE.Vector3(receiver.x, receiver.y, receiver.z));

            const geometry = new THREE.BufferGeometry().setFromPoints(points);
            const line = new THREE.Line(geometry, material);
            linesGroup.add(line);
            
            // Briefly highlight the sender and receiver nodes
            sender.mesh.material.emissive.setHex(0x58a6ff);
            receiver.mesh.material.emissive.setHex(0x2ea043);
        }
    });

    // Reset emissive glow for nodes not communicating
    nodeMap.forEach((data, rank) => {
        const isActive = activeEvents.some(e => e.sender === rank || e.receiver === rank);
        if (!isActive) {
            data.mesh.material.emissive.setHex(0x222222);
        }
    });
}

function clearLines() {
    while(linesGroup.children.length > 0){ 
        linesGroup.remove(linesGroup.children[0]); 
    }
}

// Playback logic remains exactly the same as the 2D version
function togglePlayback() {
    if (isPlaying) pausePlayback();
    else {
        isPlaying = true;
        document.getElementById("btn-play").innerHTML = "⏸ Pause";
        lastFrameTime = performance.now();
        animationFrameId = requestAnimationFrame(playLoop);
    }
}

function pausePlayback() {
    isPlaying = false;
    document.getElementById("btn-play").innerHTML = "▶ Play";
    if (animationFrameId) cancelAnimationFrame(animationFrameId);
}

let lastFrameTime = 0;
function playLoop(timestamp) {
    if (!isPlaying) return;
    const deltaTime = (timestamp - lastFrameTime) / 1000;
    lastFrameTime = timestamp;
    const speed = parseFloat(document.getElementById("speedSlider").value);
    let nextTime = currentTime + (deltaTime * speed);

    if (nextTime >= maxTime) {
        seekToTime(maxTime);
        pausePlayback();
        return;
    }
    seekToTime(nextTime);
    animationFrameId = requestAnimationFrame(playLoop);
}
