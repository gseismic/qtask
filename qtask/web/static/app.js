const { createApp } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

const app = createApp({
    data() {
        return {
            title: '任务处理系统实时监控',
            lastUpdate: '',
            activeTab: 'dashboard',
            selectedGroup: '',
            selectedNamespace: '',
            selectedTaskType: '',
            selectedStatus: '',
            
            // 统计数据
            stats: {
                todo_count: 0,
                done_count: 0,
                null_count: 0,
                error_count: 0,
                total_retries: 0
            },
            
            // 任务数据
            tasks: [],
            recentTasks: [],
            recentLimit: 10,
            groups: [],
            allNamespaces: [],
            allTaskTypes: [],
            groupStats: {},
            namespaceStats: {},
            groupsNamespace: 'default',
            
            // 图表实例
            statusChart: null,
            groupChart: null,
            
            // 创建任务表单
            newTask: {
                name: '',
                group: 'default',
                namespace: 'default',
                task_type: '',
                description: '',
                paramsJson: '{}'
            },
            
            // 错误状态
            errorCount: 0,
            
            // 图表状态
            chartsDisabled: false,
            chartInitializing: false
        }
    },
    
    computed: {
        safeGroupStats() {
            // 确保每个分组都有正确的结构
            const result = {};
            for (const [groupName, groupData] of Object.entries(this.groupStats || {})) {
                result[groupName] = {
                    total: groupData?.total || 0,
                    status_counts: groupData?.status_counts || {},
                    namespace_counts: groupData?.namespace_counts || {}
                };
            }
            return result;
        }
    },
    
    mounted() {
        // 配置axios默认设置
        axios.defaults.baseURL = window.location.origin;
        axios.defaults.timeout = 8000;
        
        this.initCharts();
        this.loadDashboardData();
        this.loadGroups();
        
        // 定时刷新数据 - 增加错误处理和降低频率
        this.refreshInterval = setInterval(() => {
            if (document.visibilityState === 'visible') {
                this.loadDashboardData();
                if (this.activeTab === 'tasks') {
                    this.loadTasks();
                }
            }
        }, 10000); // 改为10秒刷新一次
        
        // 监听窗口大小变化，调整ECharts大小
        this.resizeHandler = () => {
            if (this.statusChart) this.statusChart.resize();
            if (this.groupChart) this.groupChart.resize();
        };
        window.addEventListener('resize', this.resizeHandler);
    },
    
            beforeUnmount() {
            // 清理定时器
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
            }
            
            // 移除窗口resize监听器
            if (this.resizeHandler) {
                window.removeEventListener('resize', this.resizeHandler);
            }
            
            // 强制销毁所有图表实例
            this.destroyAllCharts();
        },
    
    methods: {
        destroyAllCharts() {
            try {
                // 使用ECharts的dispose方法销毁图表
                if (this.statusChart) {
                    this.statusChart.dispose();
                    this.statusChart = null;
                }
                if (this.groupChart) {
                    this.groupChart.dispose();
                    this.groupChart = null;
                }
            } catch (error) {
                console.warn('销毁图表时出错:', error);
            }
        },
        
        async loadDashboardData() {
            try {
                const response = await axios.get('/api/dashboard', { params: { recent: this.recentLimit }});
                const data = response.data;
                
                this.stats = data.stats;
                this.groupStats = data.group_stats;
                this.namespaceStats = data.namespace_stats || {};
                this.recentTasks = data.recent_tasks;
                this.lastUpdate = this.formatTime(data.timestamp);
                // 提取所有namespace与任务类型供筛选
                this.allNamespaces = Object.keys(this.namespaceStats || {});
                const typeSet = new Set();
                const allTasks = await axios.get('/api/tasks');
                const infos = allTasks.data?.TASK_INFOS || {};
                Object.values(infos).forEach(t => {
                    const tp = (t.data && t.data.type) ? t.data.type : undefined;
                    if (tp) typeSet.add(tp);
                });
                this.allTaskTypes = Array.from(typeSet).sort();
                
                // 延迟更新图表，确保DOM已更新
                this.$nextTick(() => {
                    // 只在仪表盘页面更新图表
                    if (this.activeTab === 'dashboard') {
                        this.updateCharts();
                    }
                });
                
                // 清除错误状态
                if (this.errorCount > 0) {
                    this.errorCount = 0;
                    ElMessage.success('连接已恢复');
                }
            } catch (error) {
                console.error('加载仪表盘数据失败:', error);
                
                // 累计错误次数，避免频繁提示
                this.errorCount = (this.errorCount || 0) + 1;
                if (this.errorCount <= 3) {
                    ElMessage.error(`网络错误 (${this.errorCount}/3): 加载数据失败`);
                } else if (this.errorCount === 4) {
                    ElMessage.warning('网络连接不稳定，已停止错误提示');
                }
            }
        },
        
        async loadTasks() {
            try {
                const response = await axios.get('/api/tasks', { params: this.selectedNamespace ? { namespace: this.selectedNamespace } : {} });
                const data = response.data;
                
                // 将所有任务合并到一个列表中，并添加详细信息
                this.tasks = [];
                const taskInfos = data.TASK_INFOS || {};
                
                // 处理各个队列的任务
                ['TODO', 'DONE', 'ERROR', 'NULL'].forEach(queue => {
                    const queueTasks = data[queue] || [];
                    
                    if (queue === 'TODO') {
                        // TODO队列包含完整任务信息
                        queueTasks.forEach(task => {
                            const taskInfo = taskInfos[task.id] || {};
                            this.tasks.push({
                                id: task.id,
                                name: taskInfo.name || '未命名任务',
                                group: taskInfo.group || 'default',
                                description: taskInfo.description || '',
                                status: taskInfo.status || 'TODO',
                                created_time: taskInfo.created_time,
                                start_time: taskInfo.start_time,
                                end_time: taskInfo.end_time,
                                processed_time: taskInfo.processed_time,
                                duration: taskInfo.duration,
                                namespace: taskInfo.namespace || 'default',
                                data: task.data
                            });
                        });
                    } else {
                        // 其他队列只有task_id
                        queueTasks.forEach(taskId => {
                            const taskInfo = taskInfos[taskId] || {};
                            this.tasks.push({
                                id: taskId,
                                name: taskInfo.name || '未命名任务',
                                group: taskInfo.group || 'default',
                                description: taskInfo.description || '',
                                status: taskInfo.status || queue,
                                created_time: taskInfo.created_time,
                                start_time: taskInfo.start_time,
                                end_time: taskInfo.end_time,
                                processed_time: taskInfo.processed_time,
                                duration: taskInfo.duration,
                                namespace: taskInfo.namespace || 'default',
                                data: taskInfo.data
                            });
                        });
                    }
                });
                
                // 按创建时间排序
                this.tasks.sort((a, b) => new Date(b.created_time) - new Date(a.created_time));
                // 前端筛选：任务类型与状态
                if (this.selectedTaskType) {
                    this.tasks = this.tasks.filter(t => this.getTaskType(t) === this.selectedTaskType);
                }
                if (this.selectedStatus) {
                    this.tasks = this.tasks.filter(t => (t.status || '') === this.selectedStatus);
                }
                
            } catch (error) {
                console.error('加载任务失败:', error);
                // 只在用户主动操作时显示错误
                if (this.activeTab === 'tasks') {
                    ElMessage.error('加载任务失败，请检查网络连接');
                }
            }
        },
        
        async loadTasksByGroup() {
            if (!this.selectedGroup) {
                this.loadTasks();
                return;
            }
            
            try {
                const response = await axios.get(`/api/tasks/group/${this.selectedGroup}`, { params: this.selectedNamespace ? { namespace: this.selectedNamespace } : {} });
                const data = response.data;
                
                this.tasks = Object.values(data.tasks).map(task => ({
                    id: task.id,
                    name: task.name || '未命名任务',
                    group: task.group || 'default',
                    description: task.description || '',
                    status: task.status || 'TODO',
                    created_time: task.created_time,
                    start_time: task.start_time,
                    end_time: task.end_time,
                    processed_time: task.processed_time,
                    duration: task.duration,
                    namespace: task.namespace || 'default',
                    data: task.data
                }));
                
                // 按创建时间排序
                this.tasks.sort((a, b) => new Date(b.created_time) - new Date(a.created_time));
                // 前端筛选：任务类型与状态
                if (this.selectedTaskType) {
                    this.tasks = this.tasks.filter(t => this.getTaskType(t) === this.selectedTaskType);
                }
                if (this.selectedStatus) {
                    this.tasks = this.tasks.filter(t => (t.status || '') === this.selectedStatus);
                }
                
            } catch (error) {
                console.error('加载分组任务失败:', error);
                ElMessage.error('加载分组任务失败');
            }
        },
        
        async loadGroups() {
            try {
                const response = await axios.get('/api/groups', { params: { namespace: this.groupsNamespace || 'default' }});
                const data = response.data;
                
                this.groups = data.groups;
                this.groupStats = data.group_stats;
            } catch (error) {
                console.error('加载分组失败:', error);
            }
        },
        
        async createTask() {
            if (!this.newTask.name || !this.newTask.task_type) {
                ElMessage.warning('请填写任务名称和类型');
                return;
            }
            
            try {
                let params = {};
                if (this.newTask.paramsJson.trim()) {
                    params = JSON.parse(this.newTask.paramsJson);
                }
                
                const taskData = {
                    name: this.newTask.name,
                    group: this.newTask.group,
                    task_type: this.newTask.task_type,
                    description: this.newTask.description,
                    params: params
                };
                
                const response = await axios.post('/api/tasks', taskData, { headers: { 'X-QTask-Namespace': this.newTask.namespace || 'default' }});
                
                ElMessage.success('任务创建成功');
                this.resetForm();
                this.loadDashboardData();
                this.loadGroups();
                
            } catch (error) {
                console.error('创建任务失败:', error);
                if (error.response?.data?.detail) {
                    ElMessage.error('创建失败: ' + error.response.data.detail);
                } else {
                    ElMessage.error('创建任务失败，请检查参数格式' + error.message);
                }
            }
        },
        
        resetForm() {
            this.newTask = {
                name: '',
                group: 'default',
                namespace: 'default',
                task_type: '',
                description: '',
                paramsJson: '{}'
            };
        },
        
        enableCharts() {
            this.chartsDisabled = false;
            this.chartInitializing = false;
            ElMessage.info('正在重新启用图表...');
            this.$nextTick(() => {
                this.initCharts();
            });
        },
        
        initCharts() {
            // 如果图表被禁用或正在初始化，跳过
            if (this.chartsDisabled || this.chartInitializing) {
                return;
            }
            
            this.chartInitializing = true;
            
            this.$nextTick(() => {
                try {
                    // 先销毁所有现有图表
                    this.destroyAllCharts();
                    
                    // 任务状态分布图 - 使用ECharts饼图
                    const statusEl = this.$refs.statusChart;
                    if (statusEl) {
                        this.statusChart = echarts.init(statusEl);
                        const statusOption = {
                            tooltip: {
                                trigger: 'item',
                                formatter: '{a} <br/>{b}: {c} ({d}%)'
                            },
                            legend: {
                                orient: 'horizontal',
                                bottom: '0%',
                                data: ['待处理', '已完成', '错误', '无效']
                            },
                            series: [{
                                name: '任务状态',
                                type: 'pie',
                                radius: ['40%', '70%'],
                                center: ['50%', '45%'],
                                data: [
                                    {value: 0, name: '待处理', itemStyle: {color: '#ffc107'}},
                                    {value: 0, name: '已完成', itemStyle: {color: '#28a745'}},
                                    {value: 0, name: '错误', itemStyle: {color: '#dc3545'}},
                                    {value: 0, name: '无效', itemStyle: {color: '#6c757d'}}
                                ],
                                emphasis: {
                                    itemStyle: {
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                                    }
                                }
                            }]
                        };
                        this.statusChart.setOption(statusOption);
                    }
                    
                    // 分组统计图 - 使用ECharts柱状图
                    const groupEl = this.$refs.groupChart;
                    if (groupEl) {
                        this.groupChart = echarts.init(groupEl);
                        const groupOption = {
                            tooltip: {
                                trigger: 'axis',
                                axisPointer: {
                                    type: 'shadow'
                                }
                            },
                            grid: {
                                left: '3%',
                                right: '4%',
                                bottom: '3%',
                                containLabel: true
                            },
                            xAxis: {
                                type: 'category',
                                data: [],
                                axisTick: {
                                    alignWithLabel: true
                                }
                            },
                            yAxis: {
                                type: 'value',
                                minInterval: 1
                            },
                            series: [{
                                name: '任务数量',
                                type: 'bar',
                                barWidth: '60%',
                                data: [],
                                itemStyle: {
                                    color: '#4bc0c0'
                                }
                            }]
                        };
                        this.groupChart.setOption(groupOption);
                    }
                } catch (error) {
                    console.error('初始化图表失败:', error);
                    // 如果图表初始化连续失败，禁用图表功能
                    this.chartsDisabled = true;
                    console.warn('图表功能已禁用以避免持续错误');
                } finally {
                    this.chartInitializing = false;
                }
            });
        },
        
        updateCharts() {
            // 如果图表被禁用或还没有初始化，跳过更新
            if (this.chartsDisabled || (!this.statusChart && !this.groupChart)) {
                return;
            }
            
            try {
                // 更新状态分布图 - ECharts
                if (this.statusChart && this.stats) {
                    const statusData = [
                        {value: this.stats.todo_count || 0, name: '待处理', itemStyle: {color: '#ffc107'}},
                        {value: this.stats.done_count || 0, name: '已完成', itemStyle: {color: '#28a745'}},
                        {value: this.stats.error_count || 0, name: '错误', itemStyle: {color: '#dc3545'}},
                        {value: this.stats.skip_count || 0, name: '跳过', itemStyle: {color: '#6c757d'}}  // 修正为skip_count
                    ];
                    
                    this.statusChart.setOption({
                        series: [{
                            name: '任务状态',
                            type: 'pie',
                            data: statusData
                        }]
                    }, false);  // 不合并选项
                }
                
                // 更新分组统计图 - ECharts
                if (this.groupChart && this.groupStats && Object.keys(this.groupStats).length > 0) {
                    const groupNames = Object.keys(this.groupStats);
                    const groupCounts = groupNames.map(name => this.groupStats[name]?.total || 0);
                    
                    this.groupChart.setOption({
                        xAxis: {
                            type: 'category',
                            data: groupNames
                        },
                        series: [{
                            name: '任务数量',
                            type: 'bar',
                            data: groupCounts
                        }]
                    }, false);  // 不合并选项
                }
            } catch (error) {
                console.error('更新图表失败:', error);
                // 图表已损坏，禁用图表功能
                this.chartsDisabled = true;
                console.warn('图表功能已禁用以避免持续错误');
            }
        },
        
        handleTabClick(tab) {
            if (tab.props.name === 'tasks') {
                this.loadTasks();
            } else if (tab.props.name === 'groups') {
                this.loadGroups();
            } else if (tab.props.name === 'dashboard') {
                // 切换到仪表盘时，确保图表正确初始化和调整大小
                this.$nextTick(() => {
                    if (!this.statusChart || !this.groupChart) {
                        this.initCharts();
                    } else {
                        // ECharts需要在容器大小变化时调用resize
                        this.statusChart?.resize();
                        this.groupChart?.resize();
                    }
                });
            }
        },
        
        refreshTasks() {
            if (this.selectedGroup) {
                this.loadTasksByGroup();
            } else {
                this.loadTasks();
            }
            ElMessage.success('刷新完成');
        },
        
        manualRefresh() {
            this.loadDashboardData();
            this.loadGroups();
            if (this.activeTab === 'tasks') {
                this.loadTasks();
            }
            ElMessage.success('数据已刷新');
        },
        
        openQueryPage() {
            window.open('/query.html', '_blank');
        },
        
        viewGroupTasks(groupName) {
            this.selectedGroup = groupName;
            this.activeTab = 'tasks';
            this.$nextTick(() => {
                this.loadTasksByGroup();
            });
        },
        
        formatTime(timeString) {
            if (!timeString) return '-';
            try {
                const date = new Date(timeString);
                return date.toLocaleString('zh-CN');
            } catch (error) {
                return timeString;
            }
        },
        
        formatDuration(task) {
            // 优先使用duration字段（但要检查是否大于0）
            if (task.duration !== null && task.duration !== undefined && task.duration > 0) {
                return this.formatTimeUnit(task.duration);
            }
            
            // 如果duration为0或null，但任务已完成，尝试使用start_time和processed_time计算
            if (task.status === 'DONE' || task.status === 'ERROR' || task.status === 'SKIP') {
                // 只使用start_time和processed_time计算真正的处理时长
                if (task.start_time && task.processed_time) {
                    try {
                        const startTime = new Date(task.start_time);
                        const endTime = new Date(task.processed_time);
                        const durationMs = endTime - startTime;
                        if (durationMs >= 0) {  // 包括0的情况
                            const durationSec = durationMs / 1000;
                            return this.formatTimeUnit(durationSec);
                        }
                    } catch (error) {
                        // 时间解析失败，使用备选方案
                        return '<1s';
                    }
                }
                
                // 如果没有start_time，对于已完成的任务显示<1s而不是"未知"
                // 因为任务确实完成了，只是时间记录有问题
                return '<1s';
            }
            
            // 如果任务还在处理中，显示状态
            if (task.status === 'PROCESSING') {
                return '处理中...';
            } else if (task.status === 'TODO') {
                return '待处理';
            }
            
            return '-';
        },
        
        formatTimeUnit(seconds) {
            // 智能选择时间单位
            if (seconds < 1) {
                return '<1s';
            } else if (seconds < 60) {
                return Math.round(seconds * 100) / 100 + 's';
            } else if (seconds < 3600) {
                return Math.round(seconds / 60 * 100) / 100 + 'min';
            } else {
                return Math.round(seconds / 3600 * 100) / 100 + 'h';
            }
        },
        
        getShortId(id) {
            return id ? id.substring(0, 8) + '...' : '-';
        },
        
        getTaskType(task) {
            // 从任务数据中提取任务类型
            if (task.data && task.data.type) {
                return task.data.type;
            }
            // 兼容旧格式
            if (task.task_type) {
                return task.task_type;
            }
            return 'unknown';
        }
    }
});

// 使用Element Plus
app.use(ElementPlus);

// 挂载应用
app.mount('#app');