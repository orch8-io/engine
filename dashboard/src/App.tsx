import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Overview from "./pages/Overview";
import Tasks from "./pages/Tasks";
import Instances from "./pages/Instances";
import InstanceDetail from "./pages/InstanceDetail";
import Sequences from "./pages/Sequences";
import SequenceDetail from "./pages/SequenceDetail";
import Settings from "./pages/Settings";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Overview />} />
        <Route path="instances" element={<Instances />} />
        <Route path="instances/:id" element={<InstanceDetail />} />
        <Route path="sequences" element={<Sequences />} />
        <Route path="sequences/:id" element={<SequenceDetail />} />
        <Route path="tasks" element={<Tasks />} />
        <Route path="settings" element={<Settings />} />
      </Route>
    </Routes>
  );
}
