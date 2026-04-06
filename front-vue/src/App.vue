<template>
  <div class="container">
    <h1 class="title">CMDB Reconciliation Platform <RocketOutlined /></h1>
    
    <a-steps :current="currentStep" class="steps">
      <a-step title="Upload Source" />
      <a-step title="Upload Target" />
      <a-step title="Define Rules" />
      <a-step title="Execution & Result" />
    </a-steps>

    <!-- STEP 0: Upload Source -->
    <a-card v-if="currentStep === 0" title="Source Data" class="card">
      <a-upload-dragger
        :multiple="false"
        :customRequest="req => customUpload(req, 'source')"
        :showUploadList="false"
      >
        <p class="ant-upload-drag-icon">
          <InboxOutlined />
        </p>
        <p class="ant-upload-text">Click or drag source file to this area to upload</p>
        <p v-if="formData.sourceFilePath" style="color: #4ade80;">Current: {{ formData.sourceFileName }}</p>
      </a-upload-dragger>
      <div class="actions">
        <a-button type="primary" :disabled="!formData.sourceFilePath" @click="currentStep++">Next Step</a-button>
      </div>
    </a-card>

    <!-- STEP 1: Upload Target -->
    <a-card v-if="currentStep === 1" title="Target Data" class="card">
      <a-upload-dragger
        :multiple="false"
        :customRequest="req => customUpload(req, 'target')"
        :showUploadList="false"
      >
        <p class="ant-upload-drag-icon">
          <InboxOutlined />
        </p>
        <p class="ant-upload-text">Click or drag target file to this area to upload</p>
        <p v-if="formData.targetFilePath" style="color: #4ade80;">Current: {{ formData.targetFileName }}</p>
      </a-upload-dragger>
      <div class="actions">
        <a-button @click="currentStep--">Previous</a-button>
        <a-button type="primary" :disabled="!formData.targetFilePath" @click="currentStep++">Next Step</a-button>
      </div>
    </a-card>

    <!-- STEP 2: Rules -->
    <a-card v-if="currentStep === 2" title="Reconciliation Rules" class="card">
      <a-form layout="vertical">
        <a-form-item label="Primary Keys (Comma separated)">
          <a-input v-model:value="pkInput" placeholder="e.g. id, ip_address" />
        </a-form-item>
        <a-form-item label="Source Filter Expression (Aviator)">
          <a-input v-model:value="formData.sourceFilterExpression" placeholder="e.g. state == 'active'" />
        </a-form-item>
        <a-form-item label="Target Filter Expression (Aviator)">
          <a-input v-model:value="formData.targetFilterExpression" placeholder="e.g. status == 'UP'" />
        </a-form-item>
        <a-form-item label="Specific Compare Fields (Comma separated, empty for all)">
          <a-input v-model:value="cfInput" placeholder="e.g. cpu_cores, memory_mb" />
        </a-form-item>
      </a-form>
      <div class="actions">
        <a-button @click="currentStep--">Previous</a-button>
        <a-button type="primary" :loading="submitting" @click="submitJob">Start Comparison <PlayCircleOutlined /></a-button>
      </div>
    </a-card>

    <!-- STEP 3: Status & Result -->
    <a-card v-if="currentStep === 3" title="Processing Task" class="card">
      <div v-if="taskStatus !== 'SUCCESS' && taskStatus !== 'FAILED'" style="text-align: center; padding: 20px;">
        <a-spin size="large" />
        <h3 style="color: white; margin-top: 20px;">Spark Batch is Running... (ID: {{ taskId }})</h3>
        
        <a-button @click="fetchLatestDownload" style="margin-top: 20px; border-color: #64748b; color: #cbd5e1" ghost>
          If polling stuck, attempt to fetch Latest Result directly
        </a-button>
      </div>
      
      <div v-if="taskStatus === 'SUCCESS'" style="text-align: center; padding: 20px;">
         <CheckCircleOutlined style="font-size: 48px; color: #4ade80;" />
         <h3 style="color: white; margin-top: 20px;">Reconciliation Completed Successfully!</h3>
         
         <a-button type="primary" size="large" @click="fetchLatestDownload" style="margin-top: 20px;">
            <DownloadOutlined /> Download Excel Report
         </a-button>
      </div>

      <div class="actions" style="margin-top: 40px; justify-content: center;">
         <a-button @click="resetForm">Run Another Task</a-button>
      </div>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onUnmounted } from 'vue';
import { message } from 'ant-design-vue';
import { InboxOutlined, RocketOutlined, PlayCircleOutlined, CheckCircleOutlined, DownloadOutlined } from '@ant-design/icons-vue';
import axios from 'axios';

const apiBase = 'http://localhost:8080/api';

const currentStep = ref(0);
const pkInput = ref('');
const cfInput = ref('');
const submitting = ref(false);
const taskId = ref('');
const taskStatus = ref('');

let pollTimer: any = null;

const formData = reactive({
  sourceFilePath: '',
  sourceFileName: '',
  targetFilePath: '',
  targetFileName: '',
  sourceFilterExpression: '',
  targetFilterExpression: '',
  outputDirPath: 'file:///data/results/'
});

const customUpload = async (options: any, type: 'source' | 'target') => {
  const { file, onSuccess, onError } = options;
  const df = new FormData();
  df.append('file', file);
  try {
    const res = await axios.post(`${apiBase}/files/upload`, df);
    if (type === 'source') {
      formData.sourceFilePath = res.data.path;
      formData.sourceFileName = res.data.fileName;
    } else {
      formData.targetFilePath = res.data.path;
      formData.targetFileName = res.data.fileName;
    }
    message.success('Upload successful!');
    onSuccess(res.data);
  } catch (err) {
    message.error('Upload failed');
    onError(err);
  }
};

const submitJob = async () => {
  if (!pkInput.value) {
    message.warning('Primary Keys are required!');
    return;
  }
  submitting.value = true;
  
  const payload = {
    sourceFilePath: formData.sourceFilePath,
    targetFilePath: formData.targetFilePath,
    primaryKeys: pkInput.value.split(',').map(s => s.trim()).filter(s => s),
    sourceFilterExpression: formData.sourceFilterExpression,
    targetFilterExpression: formData.targetFilterExpression,
    compareFields: cfInput.value.split(',').map(s => s.trim()).filter(s => s),
    outputDirPath: formData.outputDirPath
  };

  try {
    const res = await axios.post(`${apiBase}/compare/run`, payload);
    taskId.value = res.data.taskId;
    taskStatus.value = 'RUNNING';
    currentStep.value = 3;
    startPolling();
  } catch (err) {
    message.error('Failed to submit job');
  } finally {
    submitting.value = false;
  }
};

const startPolling = () => {
  pollTimer = setInterval(async () => {
    try {
      const res = await axios.get(`${apiBase}/compare/status/${taskId.value}`);
      if (res.data && res.data.status) {
         if (res.data.status !== 'RUNNING') {
           taskStatus.value = res.data.status || 'SUCCESS';
           clearInterval(pollTimer);
         }
      }
    } catch(err) {
       // Silent ignore
    }
  }, 3000); // 3 seconds
};

const fetchLatestDownload = async () => {
   try {
      const res = await axios.get(`${apiBase}/files/latest-result`);
      window.open(`${apiBase}/files/download?fileName=${res.data.fileName}`);
   } catch(err) {
      message.error('Comparison not finished yet or no result found.');
   }
};

const resetForm = () => {
  currentStep.value = 0;
  taskId.value = '';
  taskStatus.value = '';
  if (pollTimer) clearInterval(pollTimer);
};

onUnmounted(() => {
  if (pollTimer) clearInterval(pollTimer);
});
</script>

<style scoped>
.container {
  max-width: 800px;
  margin: 0 auto;
  padding: 40px 20px;
}
.title {
  color: #f8fafc;
  text-align: center;
  margin-bottom: 40px;
  font-weight: 700;
}
.steps {
  margin-bottom: 40px;
}
.steps :deep(.ant-steps-item-title) {
  color: #cbd5e1 !important;
}
.card {
  box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
  border-radius: 8px;
}
.actions {
  display: flex;
  justify-content: space-between;
  margin-top: 24px;
}
</style>
