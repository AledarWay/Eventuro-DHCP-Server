function updatePerPage(value) {
    localStorage.setItem('historyPerPage', value);
    document.getElementById('per_page_hidden').value = value;
    document.getElementById('filter-form').submit();
}

document.addEventListener('DOMContentLoaded', () => {
    const themeToggle = document.getElementById('theme-toggle');
    const currentTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-bs-theme', currentTheme);
    themeToggle.textContent = currentTheme === 'dark' ? 'Светлая тема' : 'Тёмная тема';

    themeToggle.addEventListener('click', () => {
        const newTheme = document.documentElement.getAttribute('data-bs-theme') === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-bs-theme', newTheme);
        localStorage.setItem('theme', newTheme);
        themeToggle.textContent = newTheme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
    });

    document.querySelectorAll('.sort-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const sortBy = link.getAttribute('data-sort-by');
            const sortOrder = link.getAttribute('data-sort-order');
            document.getElementById('sort_by').value = sortBy;
            document.getElementById('sort_order').value = sortOrder;
            document.getElementById('filter-form').submit();
        });
    });

    const resizers = document.querySelectorAll('.th-resize-handle');
    let currentResizer, startX, startWidth, thElement;
    resizers.forEach(resizer => {
        resizer.addEventListener('mousedown', (e) => {
            e.preventDefault();
            currentResizer = resizer;
            thElement = resizer.parentElement;
            startX = e.pageX;
            startWidth = thElement.getBoundingClientRect().width;
            currentResizer.classList.add('active');
            document.addEventListener('mousemove', resize);
            document.addEventListener('mouseup', stopResize);
        });
    });

    function resize(e) {
        if (currentResizer) {
            const newWidth = startWidth + (e.pageX - startX);
            if (newWidth >= 50 && newWidth <= 500) {
                thElement.style.width = `${newWidth}px`;
                thElement.style.minWidth = `${newWidth}px`;
                const colIndex = Array.from(thElement.parentElement.children).indexOf(thElement);
                localStorage.setItem(`historyColumnWidth_${colIndex}`, newWidth);
            }
        }
    }

    function stopResize() {
        if (currentResizer) {
            currentResizer.classList.remove('active');
            currentResizer = null;
            document.removeEventListener('mousemove', resize);
            document.removeEventListener('mouseup', stopResize);
        }
    }

    document.querySelectorAll('.history-table th').forEach((th, index) => {
        const savedWidth = localStorage.getItem(`historyColumnWidth_${index}`);
        if (savedWidth) {
            th.style.width = `${savedWidth}px`;
            th.style.minWidth = `${savedWidth}px`;
        }
    });

    const macSelect = document.getElementById('mac-select');
    const macInput = document.getElementById('mac-input');
    macSelect.addEventListener('change', () => {
        if (macSelect.value === 'manual') {
            macInput.style.display = 'block';
            macInput.name = 'mac';
            macSelect.name = '';
        } else {
            macInput.style.display = 'none';
            macInput.name = '';
            macSelect.name = 'mac';
        }
    });

    if (macSelect.value === 'manual') {
        macInput.style.display = 'block';
        macInput.name = 'mac';
        macSelect.name = '';
    } else {
        macInput.style.display = 'none';
        macInput.name = '';
        macSelect.name = 'mac';
    }

    const savedPerPage = localStorage.getItem('historyPerPage');
    const currentPerPage = document.getElementById('per_page').value;
    const perPageSelect = document.getElementById('per_page');
    const perPageHidden = document.getElementById('per_page_hidden');

    if (savedPerPage && parseInt(savedPerPage) !== parseInt(currentPerPage)) {
        perPageSelect.value = savedPerPage;
        perPageHidden.value = savedPerPage;
        setTimeout(() => {
            document.getElementById('filter-form').submit();
        }, 100);
    } else if (savedPerPage) {
        perPageSelect.value = savedPerPage;
        perPageHidden.value = savedPerPage;
    }

    perPageSelect.addEventListener('change', () => {
        updatePerPage(perPageSelect.value);
    });
});