package io.orch8.example

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import io.orch8.mobile.Orch8Client
import io.orch8.mobile.Orch8Config
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.time.Instant

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    Orch8ExampleScreen()
                }
            }
        }
    }
}

data class UiState(
    val isInitialized: Boolean = false,
    val isRunning: Boolean = false,
    val currentSequence: String? = null,
    val currentStep: String? = null,
    val logEntries: List<String> = emptyList(),
)

class Orch8ViewModel : ViewModel() {
    private val _state = MutableStateFlow(UiState())
    val state = _state.asStateFlow()

    private var client: Orch8Client? = null

    fun initializeSdk() {
        log("Initializing SDK…")
        try {
            val config = Orch8Config(
                apiUrl = "https://api.example.com",
                apiKey = "example-api-key",
                syncIntervalSecs = 300u,
                maxStoredSequences = 50u,
            )
            client = Orch8Client(config)
            _state.value = _state.value.copy(isInitialized = true)
            log("SDK initialized")
        } catch (e: Exception) {
            log("Init failed: ${e.message}")
        }
    }

    fun sync() {
        val c = client ?: return
        log("Syncing…")
        viewModelScope.launch {
            try {
                c.sync()
                log("Sync complete")
            } catch (e: Exception) {
                log("Sync failed: ${e.message}")
            }
        }
    }

    fun start() {
        val c = client ?: return
        log("Starting sequence execution…")
        viewModelScope.launch {
            try {
                c.start()
                _state.value = _state.value.copy(isRunning = true)
                log("Execution started")
            } catch (e: Exception) {
                log("Start failed: ${e.message}")
            }
        }
    }

    fun stop() {
        val c = client ?: return
        log("Stopping…")
        viewModelScope.launch {
            try {
                c.stop()
                _state.value = _state.value.copy(
                    isRunning = false,
                    currentSequence = null,
                    currentStep = null,
                )
                log("Stopped")
            } catch (e: Exception) {
                log("Stop failed: ${e.message}")
            }
        }
    }

    private fun log(message: String) {
        val ts = Instant.now().toString()
        val entries = _state.value.logEntries + "[$ts] $message"
        _state.value = _state.value.copy(logEntries = entries)
    }
}

@Composable
fun Orch8ExampleScreen(viewModel: Orch8ViewModel = viewModel()) {
    val state by viewModel.state.collectAsState()

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text("Orch8 Example", style = MaterialTheme.typography.headlineMedium)

        StatusCard(state)
        ControlButtons(state, viewModel)
        LogCard(state.logEntries)
    }
}

@Composable
private fun StatusCard(state: UiState) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("Status", style = MaterialTheme.typography.titleSmall)
            Spacer(Modifier.height(8.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                Box(
                    modifier = Modifier
                        .size(10.dp)
                        .clip(CircleShape)
                        .background(
                            if (state.isRunning) MaterialTheme.colorScheme.primary
                            else MaterialTheme.colorScheme.outline
                        )
                )
                Spacer(Modifier.width(8.dp))
                Text(if (state.isRunning) "Running" else "Stopped")
            }
            state.currentSequence?.let {
                Text("Sequence: $it", fontSize = 12.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
            state.currentStep?.let {
                Text("Step: $it", fontSize = 12.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}

@Composable
private fun ControlButtons(state: UiState, viewModel: Orch8ViewModel) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Button(
            onClick = { viewModel.initializeSdk() },
            enabled = !state.isInitialized,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Text("Initialize SDK")
        }
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            OutlinedButton(
                onClick = { viewModel.sync() },
                enabled = state.isInitialized,
                modifier = Modifier.weight(1f),
            ) {
                Text("Sync")
            }
            OutlinedButton(
                onClick = { if (state.isRunning) viewModel.stop() else viewModel.start() },
                enabled = state.isInitialized,
                modifier = Modifier.weight(1f),
            ) {
                Text(if (state.isRunning) "Stop" else "Start")
            }
        }
    }
}

@Composable
private fun LogCard(entries: List<String>) {
    Card(modifier = Modifier.fillMaxWidth().weight(1f)) {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("Log", style = MaterialTheme.typography.titleSmall)
            Spacer(Modifier.height(8.dp))
            LazyColumn {
                items(entries) { entry ->
                    Text(
                        text = entry,
                        fontSize = 11.sp,
                        fontFamily = FontFamily.Monospace,
                        lineHeight = 16.sp,
                    )
                }
            }
        }
    }
}
