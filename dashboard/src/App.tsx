import { Routes, Route } from "react-router-dom";
import { lazy } from "react";
import { ErrorBoundary } from "./components/ErrorBoundary";
import Layout from "./components/Layout";

const Overview = lazy(() => import("./pages/Overview"));
const Tasks = lazy(() => import("./pages/Tasks"));
const Workers = lazy(() => import("./pages/Workers"));
const Instances = lazy(() => import("./pages/Instances"));
const InstanceDetail = lazy(() => import("./pages/InstanceDetail"));
const Approvals = lazy(() => import("./pages/Approvals"));
const Sequences = lazy(() => import("./pages/Sequences"));
const SequenceDetail = lazy(() => import("./pages/SequenceDetail"));
const Cron = lazy(() => import("./pages/Cron"));
const Triggers = lazy(() => import("./pages/Triggers"));
const Operations = lazy(() => import("./pages/Operations"));
const Sessions = lazy(() => import("./pages/Sessions"));
const Plugins = lazy(() => import("./pages/Plugins"));
const Credentials = lazy(() => import("./pages/Credentials"));
const Pools = lazy(() => import("./pages/Pools"));
const QueueRouting = lazy(() => import("./pages/QueueRouting"));
const ApiKeys = lazy(() => import("./pages/ApiKeys"));
const RollbackPolicies = lazy(() => import("./pages/RollbackPolicies"));
const Settings = lazy(() => import("./pages/Settings"));
const MobileSync = lazy(() => import("./pages/MobileSync"));
const Usage = lazy(() => import("./pages/Usage"));
const Releases = lazy(() => import("./pages/Releases"));

export default function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Overview />} />
          <Route path="instances" element={<Instances />} />
          <Route path="instances/:id" element={<InstanceDetail />} />
          <Route path="approvals" element={<Approvals />} />
          <Route path="sequences" element={<Sequences />} />
          <Route path="sequences/:id" element={<SequenceDetail />} />
          <Route path="tasks" element={<Tasks />} />
          <Route path="workers" element={<Workers />} />
          <Route path="cron" element={<Cron />} />
          <Route path="triggers" element={<Triggers />} />
          <Route path="operations" element={<Operations />} />
          <Route path="sessions" element={<Sessions />} />
          <Route path="usage" element={<Usage />} />
          <Route path="releases" element={<Releases />} />
          <Route path="plugins" element={<Plugins />} />
          <Route path="credentials" element={<Credentials />} />
          <Route path="pools" element={<Pools />} />
          <Route path="queues" element={<QueueRouting />} />
          <Route path="mobile" element={<MobileSync />} />
          <Route path="api-keys" element={<ApiKeys />} />
          <Route path="rollback" element={<RollbackPolicies />} />
          <Route path="settings" element={<Settings />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
