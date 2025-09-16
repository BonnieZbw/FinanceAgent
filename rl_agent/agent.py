# in rl_agent/agent.py
from stable_baselines3 import PPO

class CIOAgent:
    def __init__(self, env):
        self.model = PPO("MlpPolicy", env, verbose=1)

    def train(self, total_timesteps=25000):
        self.model.learn(total_timesteps=total_timesteps)

    def predict(self, observation):
        action, _states = self.model.predict(observation)
        return action