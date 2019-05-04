import pygame
from pygame.sprite import Group

import game_functions as gf
from settings import Settings
from ship import Ship


def run_game():
    # 初始化游戏并创建屏幕
    pygame.init()
    ai_settings = Settings()
    screen = pygame.display.set_mode((ai_settings.screen_width, ai_settings.screen_height))
    pygame.display.set_caption('Alien Invasion')

    # 创建飞船
    ship = Ship(ai_settings, screen)
    # 创建子弹编组
    bullets = Group()
    # 创建外星人编组
    aliens = Group()
    gf.create_fleet(ai_settings, screen, ship, aliens)

    # 开始游戏的主循环
    while True:
        # 监听鼠标和键盘事件
        gf.check_events(ai_settings, screen, ship, bullets)
        ship.update()
        gf.update_bullets(bullets)
        gf.update_aliens(ai_settings, aliens)
        # 每次循环时都重绘屏幕
        gf.update_screen(ai_settings, screen, ship, bullets, aliens)
        # 让最近绘制的屏幕可见
        pygame.display.flip()


run_game()
