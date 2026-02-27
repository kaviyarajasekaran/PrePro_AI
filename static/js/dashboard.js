async function postJSON(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data.error || `Request failed: ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}

// Upload
const uploadForm = document.getElementById('uploadForm');
const uploadResult = document.getElementById('uploadResult');
if (uploadForm) {
  uploadForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    uploadResult.textContent = 'Uploading...';

    const fd = new FormData();
    const files = document.getElementById('files').files;
    for (const f of files) fd.append('files', f);

    try {
      const res = await fetch('/upload', { method: 'POST', body: fd });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || 'Upload failed');
      uploadResult.textContent = `Uploaded: ${data.files.join(', ')}`;

      // Helpful: auto-fill clean input with uploaded files
      const cleanInput = document.getElementById('cleanFiles');
      if (cleanInput && data.files?.length) {
        cleanInput.value = data.files.join(', ');
      }
    } catch (err) {
      uploadResult.textContent = `Error: ${err.message}`;
    }
  });
}

// Clean
const cleanBtn = document.getElementById('cleanBtn');
const cleanResult = document.getElementById('cleanResult');
if (cleanBtn) {
  cleanBtn.addEventListener('click', async () => {
    cleanResult.textContent = 'Cleaning...';
    const raw = document.getElementById('cleanFiles').value || '';
    const filenames = raw.split(',').map(s => s.trim()).filter(Boolean);

    try {
      const data = await postJSON('/clean', { filenames });
      cleanResult.textContent = `Cleaned files: ${data.cleaned_files.join(', ')}`;

      // Auto-fill summary/viz fields with first cleaned file
      if (data.cleaned_files?.length) {
        document.getElementById('summaryFile').value = data.cleaned_files[0];
        document.getElementById('vizFile').value = data.cleaned_files[0];
      }
    } catch (err) {
      cleanResult.textContent = `Error: ${err.message}`;
    }
  });
}

// Summary
const summaryBtn = document.getElementById('summaryBtn');
const summaryResult = document.getElementById('summaryResult');
if (summaryBtn) {
  summaryBtn.addEventListener('click', async () => {
    summaryResult.textContent = 'Loading summary...';
    const filename = document.getElementById('summaryFile').value.trim();

    try {
      const data = await postJSON('/summary', { filename });
      summaryResult.textContent = pretty(data);
    } catch (err) {
      summaryResult.textContent = `Error: ${err.message}`;
    }
  });
}

// Visualize
const vizBtn = document.getElementById('vizBtn');
const vizResult = document.getElementById('vizResult');
if (vizBtn) {
  vizBtn.addEventListener('click', async () => {
    vizResult.textContent = 'Creating visualization...';
    const filename = document.getElementById('vizFile').value.trim();
    const chart_type = document.getElementById('chartType').value;

    try {
      const data = await postJSON('/visualize', { filename, chart_type });
      const imgName = `${chart_type}.png`;
      const imgUrl = `/visualizations/${imgName}?t=${Date.now()}`;
      vizResult.innerHTML = `Saved: <span style="color:rgba(231,236,243,0.85)">${imgName}</span><br/><img alt="chart" src="${imgUrl}" style="max-width:100%;margin-top:10px;border-radius:14px;border:1px solid rgba(255,255,255,0.12)"/>`;
    } catch (err) {
      vizResult.textContent = `Error: ${err.message}`;
    }
  });
}
