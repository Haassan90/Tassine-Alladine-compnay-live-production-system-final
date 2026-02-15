/************************************************* 
 * 🔒 FINAL FULL SCRIPT.JS (ERPNext READY + REALTIME + ADMIN ERP ORDERS)
 * ✅ Single WebSocket
 * ✅ Machine actions update instantly
 * ✅ ETA & Next-job countdowns
 * ✅ Alerts, Metrics, CSV export
 * ✅ Login persistence
 * ✅ Filters & search
 * ✅ ERPNext Work Orders rendered in dashboard (admin only)
 * ✅ Machine rename persists across refresh
 * ✅ New jobs assigned and updated correctly
 *************************************************/

const API_BASE = "http://127.0.0.1:3333/api";
const WS_URL = "ws://127.0.0.1:3333/ws/dashboard";

/************************
 * TEMP USERS (Login)
 ************************/
const users = [
    { username: "1111", password: "1111", location: "Modan", role: "operator" },
    { username: "2222", password: "2222", location: "Baldeya", role: "operator" },
    { username: "3333", password: "3333", location: "Al-Khraj", role: "operator" },
    { username: "Admin", password: "12345", location: "all", role: "admin" }
];

let currentUser = null;
let socket = null;
let etaIntervals = {};
let nextJobIntervals = {};
let suppressNextWSRender = false;
let dashboardCache = {};
const renamedMachines = {}; // 🔹 Preserve renamed names

/************************
 * LOGIN PERSISTENCE
 ************************/
function restoreLogin() {
    const savedUser = localStorage.getItem("dashboardUser");
    if(savedUser){
        currentUser = JSON.parse(savedUser);
        document.getElementById("login-section").classList.add("hidden");
        document.getElementById("dashboard-section").classList.remove("hidden");
        initFilters();
        initWebSocket();
        loadDashboard();
        loadProductionLogs();
    }
}

function saveLogin(user){
    localStorage.setItem("dashboardUser", JSON.stringify(user));
}

function logout(){
    currentUser = null;
    localStorage.removeItem("dashboardUser");
    document.getElementById("login-section").classList.remove("hidden");
    document.getElementById("dashboard-section").classList.add("hidden");
}

/************************
 * LOGIN
 ************************/
document.getElementById("login-form").addEventListener("submit", e => {
    e.preventDefault();
    const u = document.getElementById("username").value.trim();
    const p = document.getElementById("password").value.trim();
    const user = users.find(x => x.username === u && x.password === p);
    if (!user) { alert("Invalid login"); return; }

    currentUser = user;
    saveLogin(user);

    document.getElementById("login-section").classList.add("hidden");
    document.getElementById("dashboard-section").classList.remove("hidden");

    initFilters();
    initWebSocket();
    loadDashboard();
    loadProductionLogs();
});

// Restore login on page load
restoreLogin();

/************************
 * WEBSOCKET (REALTIME)
 ************************/
function initWebSocket() {
    if (socket && socket.readyState === WebSocket.OPEN) return;

    socket = new WebSocket(WS_URL);

    socket.onopen = () => { 
        socket.send("ready"); 
        console.log("✅ WebSocket Connected"); 
        createAlert("WebSocket connected", 0); 
    };

    socket.onmessage = e => {
        try {
            const data = JSON.parse(e.data);
            dashboardCache = data;

            if(suppressNextWSRender) { suppressNextWSRender = false; return; }

            if(data.new_job) handleNewJob(data.new_job);

            if(data.locations) renderDashboard({ locations: data.locations });
            if(data.locations) updateMetricsModal({ locations: data.locations });
            if(data.work_orders) renderERPWorkOrders(data.work_orders);

            handleAlerts(data);
            loadProductionLogs();
        } catch(err) {
            console.error("WS parse error", err);
            createAlert("WebSocket data error", 2);
        }
    };

    socket.onclose = () => {
        socket = null;
        createAlert("WebSocket disconnected. Reconnecting...", 2);
        setTimeout(initWebSocket, 3000);
    };

    socket.onerror = () => socket.close();
}

/************************
 * DASHBOARD LOAD (HTTP)
 ************************/
async function loadDashboard() {
    if(!currentUser) return;
    try {
        const res = await fetch(`${API_BASE}/dashboard`);
        if(!res.ok) throw new Error("HTTP Error");
        const data = await res.json();
        dashboardCache = data;
        renderDashboard(data);
        updateMetricsModal(data);
        if(data.work_orders) renderERPWorkOrders(data.work_orders);
    } catch {
        console.error("Backend not reachable");
        createAlert("Backend not reachable. Retry in 3s",2);
        setTimeout(loadDashboard,3000);
    }
}

/************************
 * PRODUCTION LOGS
 ************************/
async function loadProductionLogs() {
    if(!currentUser) return;
    try {
        const res = await fetch(`${API_BASE}/production_logs`);
        const data = await res.json();

        const tbody = document.getElementById("logs-table");
        if (!tbody) return;

        tbody.innerHTML = "";

        data.slice(0,20).forEach(l => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${l.machine_id}</td>
                <td>${l.work_order || "N/A"}</td>
                <td>${l.pipe_size || "N/A"}</td>
                <td>${l.produced_qty}</td>
                <td>${new Date(l.timestamp).toLocaleString()}</td>
            `;
            tbody.appendChild(tr);
        });

    } catch (err) {
        console.error(err);
        createAlert("Failed to fetch production logs",2);
    }
}

/************************
 * RENDER DASHBOARD
 ************************/
function renderDashboard(data){
    if(!currentUser) return;

    const container = document.getElementById("locations");
    container.innerHTML = "";
    if(!data?.locations) return;

    let visibleLocations = currentUser.location==="all"
        ? data.locations
        : data.locations.filter(l=>l.name===currentUser.location);

    const locFilter = document.getElementById("filter-location")?.value || "all";
    const statusFilter = document.getElementById("filter-status")?.value || "all";
    const searchFilter = document.getElementById("search-machine")?.value.trim().toLowerCase() || "";

    if(locFilter!=="all")
        visibleLocations = visibleLocations.filter(l=>l.name===locFilter);

    visibleLocations.forEach(loc=>{
        let machines = loc.machines;

        if(statusFilter!=="all")
            machines = machines.filter(m=>m.status===statusFilter);

        if(searchFilter)
            machines = machines.filter(m=>
                m.name.toLowerCase().includes(searchFilter) ||
                (m.job?.work_order||"").toLowerCase().includes(searchFilter)
            );

        renderLocation(loc.name, machines);
    });

    // ✅ IMPORTANT: machine cards render hone ke baad drop activate karo
    enableMachineDropTargets();
}


function renderLocation(location, machines){
    const wrap = document.createElement("div");
    wrap.className = "location";
    wrap.innerHTML = `<h2>${location}</h2><div class="machines-grid"></div>`;
    document.getElementById("locations").appendChild(wrap);

    const grid = wrap.querySelector(".machines-grid");

    machines.forEach(m =>
        createOrUpdateMachineCard(m, location, grid)
    );
}

/************************
 * MACHINE CARD CREATE / UPDATE (FINAL OPTIMIZED + QUEUE SUPPORT)
 ************************/
function createOrUpdateMachineCard(machine, location, parentGrid){
    let card = document.getElementById(`machine-${machine.id}`);
    const displayName = renamedMachines[machine.id] || machine.name;

    const remainingTimeText = machine.job?.remaining_time != null ? formatTime(machine.job.remaining_time) : "N/A";
    const progressPercent = machine.job?.progress_percent != null ? Math.min(machine.job.progress_percent.toFixed(1),100) : 0;

    // 🔹 Next jobs HTML
    let nextJobsHTML = '';
    if(machine.next_jobs?.length){
        nextJobsHTML = `
            <div class="queue">
                <p>Next Jobs:</p>
                <ul>
                    ${machine.next_jobs.map(j => `<li>${j.work_order} (${j.pipe_size}) - ${j.total_qty || j.produced_qty}</li>`).join('')}
                </ul>
            </div>
        `;
    }

    const cardHTML = `<h3>${displayName}${currentUser.role==="admin"?`<button type="button" class="btn edit" data-location="${location}" data-id="${machine.id}" data-name="${displayName}">✏</button>`:""}</h3>
        <p>Status: <b>${machine.status.toUpperCase()}</b></p>
        ${machine.job ? `
            <div class="job-card">
                <p>WO: ${machine.job.work_order}</p>
                <p>Size: ${machine.job.size}</p>
                <p>Produced: ${machine.job.completed_qty}/${machine.job.total_qty}</p>
                <p>Remaining: <span id="eta-${machine.id}">${remainingTimeText}</span></p>
                <p>Progress: ${progressPercent}%</p>
                <div class="progress-bar-container">
                    <div class="progress-bar" style="width:${progressPercent}%;background-color:${progressPercent>=90?'red':progressPercent>=75?'orange':'green'}"></div>
                </div>
            </div>` : `<div class="job-card"><p>No Job</p></div>`}
        ${nextJobsHTML}  <!-- 🔹 Queue added -->
        ${currentUser.role==="operator" ? `
            <div class="controls">
                <button type="button" class="btn start" data-location="${location}" data-id="${machine.id}">▶</button>
                <button type="button" class="btn pause" data-location="${location}" data-id="${machine.id}">⏸</button>
                <button type="button" class="btn stop" data-location="${location}" data-id="${machine.id}">⛔</button>
            </div>`:""}`;

    // ✅ Add or update card
    if(card){
        card.innerHTML = cardHTML;
        card.className = `machine status-${machine.status}`;
    } else {
        card = document.createElement("div");
        card.className = `machine status-${machine.status}`;
        card.id = `machine-${machine.id}`;
        card.dataset.location = location;
        card.dataset.id = machine.id;

        card.innerHTML = cardHTML;
        parentGrid.appendChild(card);
    }

    // ✅ ETA countdown
    if(machine.job?.remaining_time != null) setupETACountdown(machine.id, machine.job.remaining_time);

    // ✅ Attach drag/drop for ERPNext jobs (operator/admin)
    if(currentUser.role==="operator" || currentUser.role==="admin"){
        attachMachineDrop(card, machine.id, location);
    }
}

/************************
 /************************
 * ATTACH DRAG/DROP TO MACHINE CARD (QUEUE READY)
 ************************/
function attachMachineDrop(card, machineId, location){
    if(card.hasAttribute("data-drag-listener")) return;
    card.setAttribute("data-drag-listener","true");

    card.addEventListener("dragover", e => { 
        e.preventDefault(); 
        card.classList.add("drag-hover"); 
    });

    card.addEventListener("dragleave", () => { 
        card.classList.remove("drag-hover"); 
    });

    card.addEventListener("drop", async e => {
        e.preventDefault(); 
        card.classList.remove("drag-hover");

        try {
            const d = JSON.parse(e.dataTransfer.getData("text/plain"));
            // d should contain: { work_order, pipe_size, total_qty, eta }

            // Find the machine in dashboard cache
            const machine = dashboardCache.locations
                .flatMap(l => l.machines)
                .find(m => m.id === machineId);

            if(!machine) throw new Error("Machine not found");

            // Assign job or queue it
            if(!machine.job || machine.status === "idle"){
                machine.job = {
                    work_order: d.work_order,
                    size: d.pipe_size,
                    completed_qty: 0,
                    total_qty: d.total_qty,
                    remaining_time: d.eta || 0,
                    progress_percent: 0
                };
            } else {
                machine.next_jobs = machine.next_jobs || [];
                machine.next_jobs.push({
                    work_order: d.work_order,
                    pipe_size: d.pipe_size,
                    produced_qty: 0,
                    total_qty: d.total_qty,
                    remaining_time: d.eta || 0
                });
            }

            // Send assignment to backend
            const payload = {
                work_order: d.work_order,
                pipe_size: d.pipe_size,
                target_qty: d.total_qty,
                location,
                machine_id: machineId,
                role: currentUser.role
            };

            const res = await fetch(`${API_BASE}/machine/assign`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            const result = await res.json();

            if(result.ok){
                createAlert(`✅ Job ${d.work_order} assigned`, 0);
                suppressNextWSRender = true; // prevent duplicate WS render
            } else {
                createAlert("❌ Assignment failed", 2);
            }

            // Re-render dashboard to reflect queued job
            loadDashboard();

        } catch(err){
            console.error(err);
            createAlert("❌ Drop error: " + err.message, 2);
        }
    });
}

/************************
 * MACHINE RENAME
 ************************/
async function editMachineName(e, location, machineId, oldName){
    e.preventDefault(); e.stopPropagation();
    const newName = prompt("Enter new machine name:", oldName);
    if(!newName || newName.trim()===oldName) return;

    try{
        const res = await fetch(`${API_BASE}/machine/rename`,{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({location,machine_id:machineId,new_name:newName.trim()})
        });
        const data = await res.json();
        if(!data.ok) { alert("❌ Rename failed"); return; }

        renamedMachines[machineId] = newName.trim();
        const card = document.getElementById(`machine-${machineId}`);
        if(card){ const h3 = card.querySelector("h3"); h3.childNodes[0].nodeValue = newName + " "; }
        createAlert(`✅ Machine renamed to ${newName}`,0);
    } catch(err){ console.error(err); alert("❌ Rename error"); }
}

/************************
 * MACHINE ACTIONS (INSTANT UPDATE)
 ************************/
async function machineAction(e, action, location, id){
    e.preventDefault();
    e.stopPropagation();

    try {
        const res = await fetch(`${API_BASE}/machine/${action}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ location, machine_id: id })
        });

        const data = await res.json();

        if (!data.ok) { alert(`❌ ${action} failed`); return; }

        const card = document.getElementById(`machine-${id}`);
        if(card && data.machine){
            const grid = card.parentElement;
            createOrUpdateMachineCard(data.machine, location, grid);
        }

    } catch (err) {
        console.error(err);
        alert(`❌ ${action} failed`);
    }
}

/************************
 * ETA COUNTDOWN
 ************************/
function formatTime(sec){ 
    if(!sec||sec<0) return "0:00"; 
    const m = Math.floor(sec/60); 
    const s = Math.floor(sec%60); 
    return `${m}:${s.toString().padStart(2,'0')}`;
}

function setupETACountdown(id, sec){
    clearInterval(etaIntervals[id]);
    let t = sec;
    const el = document.getElementById(`eta-${id}`);
    if(!el) return;

    el.textContent = formatTime(t);
    etaIntervals[id] = setInterval(() => {
        if(!el) { clearInterval(etaIntervals[id]); return; }
        el.textContent = formatTime(t--);
        if(t < 0) clearInterval(etaIntervals[id]);
    }, 1000);
}

/************************
 * ALERTS PANEL
 ************************/
function handleAlerts(data){
    const alertsContainer = document.getElementById("alerts"); 
    if(!alertsContainer) return; 
    alertsContainer.innerHTML="";
    data.locations.forEach(loc=>{
        loc.machines.forEach(m=>{
            if(m.job){
                const p = m.job.progress_percent;
                if(p>=75 && p<90) createAlert(`${renamedMachines[m.id]||m.name} reached 75% progress!`,1);
                else if(p>=90 && p<100) createAlert(`${renamedMachines[m.id]||m.name} reached 90% progress!`,2);
                else if(p>=100) createAlert(`${renamedMachines[m.id]||m.name} completed!`,3);
            }
        });
    });
}

function createAlert(message,level){
    const alertsContainer = document.getElementById("alerts"); 
    if(!alertsContainer) return;
    const alertDiv = document.createElement("div");
    alertDiv.className = "alert"; 
    alertDiv.style.backgroundColor = level===3?"#2196f3":level===2?"#f44336":"#ff9800"; 
    alertDiv.textContent = message; 
    alertsContainer.prepend(alertDiv); 
    setTimeout(()=>alertDiv.remove(),10000);
}

/************************
 * METRICS MODAL
 ************************/
function updateMetricsModal(data){
    const modal = document.getElementById("metrics-modal");
    if(!modal) return;
    const tbody = modal.querySelector("tbody"); tbody.innerHTML="";
    data.locations.forEach(loc=>{loc.machines.forEach(m=>{
        const tr=document.createElement("tr");
        const displayName = renamedMachines[m.id] || m.name; // show renamed name
        tr.innerHTML=`<td>${loc.name}</td><td>${displayName}</td><td>${m.job?m.job.work_order:"N/A"}</td><td>${m.job?m.job.size:"-"}</td><td>${m.job?m.job.completed_qty:0}</td><td>${m.job?m.job.total_qty:0}</td><td>${m.job?m.job.progress_percent:0}</td><td>${m.status}</td>`;
        tbody.appendChild(tr);
    })});
}
function openMetricsModal(){document.getElementById("metrics-modal")?.classList.remove("hidden");}
function closeMetricsModal(){document.getElementById("metrics-modal")?.classList.add("hidden");}
function exportTableToCSV(tableId, filename='export.csv'){const table=document.getElementById(tableId); const rows=Array.from(table.querySelectorAll('tr')); const csv=rows.map(r=>Array.from(r.querySelectorAll('th,td')).map(c=>`"${c.textContent}"`).join(',')).join('\n'); const blob=new Blob([csv],{type:'text/csv'}); const link=document.createElement('a'); link.href=URL.createObjectURL(blob); link.download=filename; link.click();}

/************************
 * FILTERS
 ************************/
function initFilters(){
    const locSelect=document.getElementById("filter-location");
    if(!locSelect) return;
    locSelect.innerHTML=`<option value="all">All</option>`;
    users.filter(u=>u.location!=="all").forEach(u=>{
        if(!Array.from(locSelect.options).some(o=>o.value===u.location)) locSelect.innerHTML+=`<option value="${u.location}">${u.location}</option>`;
    });
    locSelect.addEventListener("change",loadDashboard);
    document.getElementById("filter-status").addEventListener("change",loadDashboard);
    document.getElementById("search-machine").addEventListener("input",loadDashboard);
}

/************************
 * EVENT DELEGATION
 ************************/
document.addEventListener("click",function(e){
    const btn = e.target.closest("button"); if(!btn) return;

    if(btn.classList.contains("start")) machineAction(e,'start',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("pause")) machineAction(e,'pause',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("stop")) machineAction(e,'stop',btn.dataset.location,btn.dataset.id);
    else if(btn.classList.contains("edit")) editMachineName(e,btn.dataset.location,btn.dataset.id,btn.dataset.name);
    else if(btn.id==="btn-view-metrics") openMetricsModal();
    else if(btn.id==="btn-close-metrics") closeMetricsModal();
    else if(btn.id==="btn-export-csv") exportTableToCSV('metrics-table','metrics.csv');
});

// Close WS on page unload
window.addEventListener("beforeunload",()=>{if(socket) socket.close();});

/************************
 * NEW JOB ALERT / ASSIGNMENT
 ************************/
function handleNewJob(job){
    const { machine_id, work_order, qty, pipe_size, eta, machine_name } = job;

    for(const loc of dashboardCache.locations){
        const machine = loc.machines.find(m => m.id === machine_id);
        if(machine){
            if(!machine.job || machine.status === "idle"){
                machine.job = {
                    work_order: work_order,
                    size: pipe_size,
                    completed_qty: 0,
                    total_qty: qty,
                    remaining_time: eta,
                    progress_percent:0
                };
            } else {
                machine.next_job = {
                    work_order: work_order,
                    pipe_size: pipe_size,
                    produced_qty: 0,
                    total_qty: qty,
                    remaining_time: eta
                };
            }

            const machineEl = document.getElementById(`machine-${machine_id}`);
            if(machineEl){
                const grid = machineEl.parentElement;
                createOrUpdateMachineCard(machine, loc.name, grid);
            }

            break;
        }
    }

    createAlert(`✅ New Job Assigned to ${renamedMachines[machine_id]||machine_name}: ${work_order}`, 0);
}

/************************
 * ERPNext WORK ORDERS (ADMIN ONLY)
 ************************/
async function loadERPNextWorkOrders(){
    if(!currentUser || currentUser.role !== "admin") return;

    try {
        const res = await fetch(`${API_BASE}/admin/work_orders`);
        if(!res.ok) throw new Error("Failed to fetch ERPNext work orders");
        const data = await res.json();
        renderERPWorkOrders(data.work_orders || []);
    } catch(err){
        console.error(err);
        createAlert("Failed to load ERPNext Work Orders", 2);
        setTimeout(loadERPNextWorkOrders, 5000);
    }
}

function renderERPWorkOrders(orders){
    const container = document
        .getElementById("erp-workorders-table")
        ?.querySelector("tbody");

    if(!container) return;

    container.innerHTML = "";

    orders.forEach(o => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${o.id}</td>
            <td>${o.pipe_size || "-"}</td>
            <td>${o.qty}</td>
            <td>${o.produced_qty || 0}</td>
            <td>${o.status}</td>
            <td>${o.machine_id || "N/A"}</td>
        `;
        container.appendChild(tr);
    });

    // 🔥 Re-enable drag after render
    enableERPDragRows();
}

/************************
 * DRAG SOURCE (ERP ROWS)
 ************************/
function enableERPDragRows(){
    const rows = document.querySelectorAll("#erp-workorders-table tbody tr");

    rows.forEach(row=>{
        row.setAttribute("draggable", true);

        row.addEventListener("dragstart", e=>{
            const work_order = row.children[0]?.textContent;
            const pipe_size  = row.children[1]?.textContent;
            const qty        = parseInt(row.children[2]?.textContent || 0);

            e.dataTransfer.setData("text/plain", JSON.stringify({
                work_order,
                pipe_size,
                total_qty: qty
            }));
        });
    });
}
/************************
 * DROP TARGET (MACHINES)
 ************************/
function enableMachineDropTargets(){

    if(!currentUser || currentUser.role !== "operator") return;

    document.querySelectorAll(".machine").forEach(card=>{

        card.addEventListener("dragover", e=>{
            e.preventDefault();
            card.classList.add("drag-hover");
        });

        card.addEventListener("dragleave", ()=>{
            card.classList.remove("drag-hover");
        });

        card.addEventListener("drop", async e=>{
            e.preventDefault();
            card.classList.remove("drag-hover");

            const data = JSON.parse(
                e.dataTransfer.getData("text/plain")
            );

            const payload = {
                work_order: data.work_order,
                pipe_size: data.pipe_size,
                target_qty: data.total_qty,
                location: card.dataset.location,
                machine_id: card.dataset.id,
                role: currentUser.role
            };

            try{
                const res = await fetch(`${API_BASE}/machine/assign`,{
                    method:"POST",
                    headers:{"Content-Type":"application/json"},
                    body: JSON.stringify(payload)
                });

                const result = await res.json();

                if(result.ok){
                    createAlert(`✅ Job ${data.work_order} assigned`,0);
                    suppressNextWSRender = true;
                }else{
                    createAlert("❌ Assignment failed",2);
                }
            }catch(err){
                console.error(err);
                createAlert("❌ Assign error",2);
            }
        });
    });
}
