document.addEventListener('DOMContentLoaded', () => {
    const tempValueEl = document.getElementById('tempValue');
    const tempBarEl = document.getElementById('tempBar');
    const humidityValueEl = document.getElementById('humidityValue');
    const humidityBarEl = document.getElementById('humidityBar');
    const statusBadgeEl = document.getElementById('statusBadge');
    const msgCountEl = document.getElementById('msgCount');
    const sensorIdEl = document.getElementById('sensorId');
    const locationEl = document.getElementById('location');
    const timestampEl = document.getElementById('timestamp');
    const pulseDotEl = document.querySelector('.pulse-dot');
    const statusTextEl = document.getElementById('statusText');

    let lastTimestamp = '';

    async function fetchLatestReading() {
        try {
            // Fetch latest reading saved by consumer.py
            const response = await fetch('latest_reading.json?t=' + Date.now());
            if (!response.ok) {
                throw new Error('Data file not ready yet');
            }
            const data = await response.json();

            // Update UI elements
            tempValueEl.textContent = data.temperature.toFixed(1);
            const tempPercentage = Math.min(Math.max(((data.temperature - 20) / 20) * 100, 0), 100);
            tempBarEl.style.width = `${tempPercentage}%`;

            humidityValueEl.textContent = data.humidity;
            humidityBarEl.style.width = `${data.humidity}%`;

            msgCountEl.textContent = data.total_messages || 0;
            sensorIdEl.textContent = data.sensor_id || '--';
            locationEl.textContent = data.location || '--';
            timestampEl.textContent = data.timestamp || '--';

            // Update Status Badge
            const status = (data.status || 'NORMAL').toUpperCase();
            statusBadgeEl.textContent = status;
            statusBadgeEl.className = 'status-badge';
            if (status === 'NORMAL') {
                statusBadgeEl.classList.add('status-normal');
            } else if (status === 'WARM') {
                statusBadgeEl.classList.add('status-warm');
            } else if (status === 'HIGH') {
                statusBadgeEl.classList.add('status-high');
            } else {
                statusBadgeEl.classList.add('status-unknown');
            }

            // Connection active indicator
            if (data.timestamp !== lastTimestamp) {
                lastTimestamp = data.timestamp;
                pulseDotEl.classList.add('active');
                statusTextEl.textContent = 'Kafka Stream Active';
            }
        } catch (error) {
            statusTextEl.textContent = 'Waiting for Producer/Consumer stream...';
            pulseDotEl.classList.remove('active');
        }
    }

    // Poll every 1 second
    fetchLatestReading();
    setInterval(fetchLatestReading, 1000);
});
