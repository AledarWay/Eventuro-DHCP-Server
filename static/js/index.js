function updatePerPage(value) {
    localStorage.setItem('indexPerPage', value);
    document.getElementById('per_page_hidden').value = value;
    document.getElementById('filter-form').submit();
}

document.addEventListener('DOMContentLoaded', () => {
    const body = document.body;
    const getFreeIpUrl = body.dataset.getFreeIpUrl;
    const logsUrl = body.dataset.logsUrl;

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

    document.querySelectorAll('.toggle-details').forEach(button => {
        button.addEventListener('click', () => {
            const icon = button.querySelector('i');
            icon.classList.toggle('bi-chevron-right');
            icon.classList.toggle('bi-chevron-down');
        });
    });

    document.querySelectorAll('.editable-cell.hostname-cell').forEach(cell => {
        cell.addEventListener('click', () => {
            const parentTd = cell.parentElement;
            const editForm = parentTd.querySelector('.edit-field.hostname-edit');
            cell.classList.add('d-none');
            editForm.classList.remove('d-none');
            editForm.querySelector('input').focus();
        });
    });

    document.querySelectorAll('.editable-cell.ip-cell').forEach(cell => {
        cell.addEventListener('click', () => {
            const parentTd = cell.parentElement;
            const editForm = parentTd.querySelector('.edit-field.ip-edit');
            const input = editForm.querySelector('input[name="ip"]');
            const currentValue = cell.getAttribute('data-value');

            if (currentValue === '-') {
                fetch(getFreeIpUrl)
                    .then(response => response.json())
                    .then(data => {
                        if (data.ip) {
                            input.value = data.ip;
                        } else {
                            alert(data.error || 'Не удалось получить свободный IP');
                            input.value = '';
                        }
                        cell.classList.add('d-none');
                        editForm.classList.remove('d-none');
                        input.focus();
                    })
                    .catch(error => {
                        console.error('Ошибка при получении свободного IP:', error);
                        alert('Ошибка при получении свободного IP');
                        input.value = '';
                        cell.classList.add('d-none');
                        editForm.classList.remove('d-none');
                        input.focus();
                    });
            } else {
                cell.classList.add('d-none');
                editForm.classList.remove('d-none');
                input.focus();
            }
        });
    });

    document.querySelectorAll('.cancel-edit').forEach(button => {
        button.addEventListener('click', () => {
            const editForm = button.closest('.edit-field');
            const cell = editForm.previousElementSibling;
            editForm.classList.add('d-none');
            cell.classList.remove('d-none');
        });
    });

    document.querySelectorAll('.block-device-btn').forEach(button => {
        button.addEventListener('click', () => {
            const mac = button.getAttribute('data-mac');
            document.getElementById('block-mac').textContent = mac;
            document.querySelector('.confirm-block').onclick = () => {
                document.getElementById(`block-device-form-${mac}`).submit();
            };
        });
    });

    document.querySelectorAll('.reset-lease-btn').forEach(button => {
        button.addEventListener('click', () => {
            const mac = button.getAttribute('data-mac');
            document.getElementById('reset-mac').textContent = mac;
            document.querySelector('.confirm-reset').onclick = () => {
                document.getElementById(`reset-lease-form-${mac}`).submit();
            };
        });
    });

    document.querySelectorAll('.delete-lease-btn').forEach(button => {
        button.addEventListener('click', () => {
            const mac = button.getAttribute('data-mac');
            document.getElementById('delete-mac').textContent = mac;
            document.querySelector('.confirm-delete').onclick = () => {
                document.getElementById(`delete-lease-form-${mac}`).submit();
            };
        });
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

    const logsModal = document.getElementById('logsModal');
    logsModal.addEventListener('show.bs.modal', () => {
        const logsContent = document.getElementById('logs-content');
        logsContent.textContent = 'Загрузка логов...';
        fetch(logsUrl)
            .then(response => response.json())
            .then(data => {
                if (data.logs) {
                    logsContent.textContent = data.logs;
                } else {
                    logsContent.textContent = data.error || 'Не удалось загрузить логи.';
                }
                setTimeout(() => {
                    logsContent.scrollTop = logsContent.scrollHeight;
                }, 50);
            })
            .catch(() => {
                logsContent.textContent = 'Ошибка загрузки логов.';
                setTimeout(() => {
                    logsContent.scrollTop = logsContent.scrollHeight;
                }, 50);
            });
    });

    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));

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
                localStorage.setItem(`columnWidth_${colIndex}`, newWidth);
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

    document.querySelectorAll('.stats-table th').forEach((th, index) => {
        const savedWidth = localStorage.getItem(`columnWidth_${index}`);
        if (savedWidth) {
            th.style.width = `${savedWidth}px`;
            th.style.minWidth = `${savedWidth}px`;
        }
    });

    const savedPerPage = localStorage.getItem('indexPerPage');
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